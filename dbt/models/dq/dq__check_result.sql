{{ config(materialized='table') }}

-- 데이터 품질 판정. grain = check_name × check_date × source_name.
-- **멱등** — 같은 입력이면 언제 돌려도 같은 판정이다. 통보 여부 같은 상태는
-- 여기 없다(그건 dq__alert_dispatch 의 책임). price-integrity 의
-- anomaly_event / notification_dispatch 분리와 같은 형태.
--
-- ★ status 에 'insufficient_data' 가 있는 이유
--
-- 볼륨 급감은 트레일링 중앙값이 있어야 판정할 수 있는데, 파이프라인을 막 시작하면
-- 그 이력이 없다. 이때 조용히 'ok' 로 처리하면 **검사를 안 하면서 검사한 척**하게
-- 된다. 그건 체크가 없는 것보다 나쁘다 — 있다고 믿고 안 보게 되니까.
-- 그래서 판정 불가를 별도 상태로 노출하고, 알림에도 그대로 드러낸다.
--
-- 이력이 필요 없는 체크를 앞쪽에 배치했다. 가장 중요한 실패(수집이 조용히 0건이
-- 되는 것)는 1일차부터 잡힌다.

with runs as (

    select
        source_name,
        started_at::date as run_date,
        status,
        rows_fetched,
        rows_new,
        watermark_before,
        watermark_after,
        error_msg
    from {{ source('lake', 'ops__ingest_run_log') }}
    qualify row_number() over (
        partition by source_name, started_at::date
        order by started_at desc
    ) = 1   -- 하루 여러 번 돌았으면 마지막 실행을 그날의 결과로 본다

),

latest_run as (
    select * from runs
    qualify row_number() over (partition by source_name order by run_date desc) = 1
),

watermarks as (
    select source_name, cursor_ts, consecutive_fail, last_success_at
    from {{ source('lake', 'ops__source_watermark') }}
),

-- ── 1. 수집 실패 (이력 불필요) ────────────────────────────────
check_ingest_error as (

    select
        'ingest_error'                        as check_name,
        w.source_name,
        case
            when w.consecutive_fail >= 2      then 'critical'
            when w.consecutive_fail = 1       then 'warn'
            when r.status = 'error'           then 'critical'
            else 'ok'
        end                                   as status,
        w.consecutive_fail::DOUBLE            as observed_value,
        0.0                                   as expected_value,
        case
            when w.consecutive_fail > 0 then '연속 실패 ' || w.consecutive_fail || '회'
            when r.status = 'error' then coalesce(r.error_msg, '수집 실패')
            else '정상'
        end                                   as detail
    from watermarks w
    left join latest_run r on r.source_name = w.source_name

),

-- ── 2. 볼륨 0 (이력 불필요) ──────────────────────────────────
-- 가장 중요한 체크. "API 는 응답하는데 아무것도 안 준다" 를 기준선 없이 잡는다.
check_volume_zero as (

    select
        'volume_zero'                         as check_name,
        source_name,
        case when status = 'ok' and coalesce(rows_fetched, 0) = 0
             then 'critical' else 'ok' end    as status,
        coalesce(rows_fetched, 0)::DOUBLE     as observed_value,
        null::DOUBLE                          as expected_value,
        case when status = 'ok' and coalesce(rows_fetched, 0) = 0
             then '성공으로 기록됐으나 수신 0건 — 조용한 중단'
             else '수신 ' || coalesce(rows_fetched, 0) || '건' end as detail
    from latest_run

),

-- ── 3. Freshness (이력 불필요) ───────────────────────────────
check_freshness as (

    select
        'freshness'                           as check_name,
        source_name,
        case
            when last_success_at is null then 'insufficient_data'
            when date_diff('hour', last_success_at, now()) >= {{ var('dq_freshness_critical_hours') }} then 'critical'
            when date_diff('hour', last_success_at, now()) >= {{ var('dq_freshness_warn_hours') }} then 'warn'
            else 'ok'
        end                                   as status,
        date_diff('hour', last_success_at, now())::DOUBLE as observed_value,
        {{ var('dq_freshness_warn_hours') }}::DOUBLE      as expected_value,
        case when last_success_at is null then '성공 이력 없음'
             else '마지막 성공 ' || date_diff('hour', last_success_at, now()) || '시간 전' end as detail
    from watermarks

),

-- ── 4. 워터마크 정체 (실행 2회 이상 필요) ─────────────────────
-- 커서가 있는 소스만 대상. HN 은 mutating 소스라 커서를 안 쓴다.
check_watermark_stall as (

    select
        'watermark_stall'                     as check_name,
        source_name,
        case
            when run_count < 2 then 'insufficient_data'
            when watermark_before = watermark_after and rows_new = 0 then 'warn'
            else 'ok'
        end                                   as status,
        run_count::DOUBLE                     as observed_value,
        null::DOUBLE                          as expected_value,
        case
            when run_count < 2 then '실행 ' || run_count || '회 — 판정 불가'
            when watermark_before = watermark_after and rows_new = 0
                then '커서 미전진 + 신규 0건 — 무증상 정체 의심'
            else '커서 정상 전진'
        end                                   as detail
    from (
        select
            r.*,
            count(*) over (partition by r.source_name) as run_count
        from runs r
        where r.watermark_after is not null
        qualify row_number() over (partition by r.source_name order by r.run_date desc) = 1
    )

),

-- ── 5. 볼륨 급감 (관측 N일 이상 필요) ────────────────────────
volume_history as (

    select
        source_name,
        run_date,
        rows_new,
        -- 요일 효과 보정: arXiv 는 주말 제출이 평일보다 적다. 평일/주말을
        -- 섞어서 중앙값을 내면 월요일마다 '급감' 오탐이 난다.
        case when dayofweek(run_date) in (0, 6) then 'weekend' else 'weekday' end as day_type
    from runs
    where status = 'ok'

),

volume_baseline as (

    select
        h.source_name,
        h.run_date,
        h.rows_new,
        h.day_type,
        (select median(p.rows_new)
         from volume_history p
         where p.source_name = h.source_name
           and p.day_type = h.day_type
           and p.run_date < h.run_date
           and p.run_date >= h.run_date - interval 28 day) as baseline_median,
        (select count(*)
         from volume_history p
         where p.source_name = h.source_name
           and p.day_type = h.day_type
           and p.run_date < h.run_date
           and p.run_date >= h.run_date - interval 28 day) as baseline_n
    from volume_history h
    qualify row_number() over (partition by h.source_name order by h.run_date desc) = 1

),

check_volume_drop as (

    select
        'volume_drop'                         as check_name,
        source_name,
        case
            when baseline_n < {{ var('dq_volume_min_observations') }} then 'insufficient_data'
            when rows_new < baseline_median * {{ var('dq_volume_drop_ratio') }} then 'warn'
            else 'ok'
        end                                   as status,
        rows_new::DOUBLE                      as observed_value,
        baseline_median::DOUBLE               as expected_value,
        case
            when baseline_n < {{ var('dq_volume_min_observations') }}
                then '동일 요일유형 관측 ' || baseline_n || '일 — 기준선 부족(최소 '
                     || {{ var('dq_volume_min_observations') }} || '일)'
            else '신규 ' || rows_new || '건 (' || day_type || ' 중앙값 ' || baseline_median || ')'
        end                                   as detail
    from volume_baseline

),

-- ── 6. 토픽 독식 (이력 불필요) ───────────────────────────────
-- 관심 프로필과 코퍼스가 어긋나면 한 토픽이 매칭을 독식한다.
-- 2026-09-08 에 'LLM 활용'이 69% 를 먹은 걸 사람이 눈으로 발견했는데,
-- 그런 건 자동으로 잡혀야 한다.
topic_share as (
    select
        best_interest_label,
        count(*)::DOUBLE / sum(count(*)) over () as share
    from {{ ref('gold__document_affinity') }}
    group by 1
    qualify row_number() over (order by count(*) desc) = 1
),

check_topic_domination as (

    select
        'topic_domination'                    as check_name,
        '(전체)'                               as source_name,
        case when share > {{ var('dq_topic_domination_ratio') }} then 'warn' else 'ok' end as status,
        round(share, 3)                       as observed_value,
        {{ var('dq_topic_domination_ratio') }}::DOUBLE as expected_value,
        best_interest_label || ' 토픽이 ' || round(share * 100, 1) || '% 차지' as detail
    from topic_share

),

-- ── 7. 중복 후보 (이력 불필요) ───────────────────────────────
-- doc_id 는 다른데 제목이 같은 쌍. unique 테스트(블로킹)와 달리 이건 소스 간
-- 교차 중복이라 정상일 수 있어 경고만 한다 — 다이제스트에 같은 내용이 두 번
-- 나가는 걸 막으려면 알고는 있어야 한다.
check_duplicate as (

    select
        'duplicate_candidate'                 as check_name,
        '(전체)'                               as source_name,
        case when count(*) > 0 then 'warn' else 'ok' end as status,
        count(*)::DOUBLE                      as observed_value,
        0.0                                   as expected_value,
        case when count(*) > 0
             then '제목이 같은 서로 다른 문서 ' || count(*) || '쌍'
             else '중복 없음' end             as detail
    from (
        select lower(trim(title)) as norm_title
        from {{ ref('silver__document') }}
        where title is not null
        group by 1 having count(distinct doc_id) > 1
    )

),

unioned as (
    select * from check_ingest_error
    union all select * from check_volume_zero
    union all select * from check_freshness
    union all select * from check_watermark_stall
    union all select * from check_volume_drop
    union all select * from check_topic_domination
    union all select * from check_duplicate
)

select
    check_name,
    current_date as check_date,
    source_name,
    status,
    observed_value,
    expected_value,
    detail,
    now()        as evaluated_at
from unioned
