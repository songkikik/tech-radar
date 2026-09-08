{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
  )
}}

-- 통보할 DQ 경고 목록. **비멱등 append-only** — "언제 무엇을 알렸나" 라는 사실 기록.
--
-- dq__check_result(멱등 판정)와 분리하는 이유: 판정은 언제 돌려도 같아야 하고,
-- 통보는 한 번 나가면 되돌릴 수 없다. 둘을 한 테이블에 두면 재실행할 때마다
-- 판정이 바뀌거나 중복 통보가 나간다. price-integrity 의
-- anomaly_event / notification_dispatch 분리와 같은 형태다.
--
-- ★ cooldown 을 severity 별로 나누는 이유
--   같은 체크를 매일 반복 통보하면 알림 피로가 쌓이고, 그러면 진짜 경고를 놓친다.
--   그런데 warn 이 cooldown 중이라는 이유로 critical 까지 막히면 안 된다.
--   그래서 파티션에 severity 를 넣어 severity 별로 독립된 cooldown 을 갖게 한다
--   (price-integrity 에서 lagInFrame 으로 구현했던 것을 lag 으로 옮긴 것).
--
-- insufficient_data 는 통보하지 않는다. 파이프라인 초기에는 대부분이 그 상태라
-- 매일 15건씩 알림이 가면 아무도 안 본다. 대신 다이제스트 배너에 요약해서
-- "지금 몇 개가 판정 불가" 라고만 알린다.

with alertable as (

    select
        check_name,
        check_date,
        source_name,
        status,
        observed_value,
        expected_value,
        detail,
        evaluated_at
    from {{ ref('dq__check_result') }}
    where status in ('warn', 'critical')

),

{% if is_incremental() %}
last_sent as (

    -- 같은 (체크 × 소스 × severity) 의 마지막 통보 시각.
    --
    -- ⚠️ 이 테이블의 컬럼명은 severity 다(check_result 쪽은 status). 아래에서
    --    status 로 조인하므로 여기서 별칭을 맞춰준다. 이름이 어긋나 있어
    --    incremental 2회차에서 Binder Error 가 났었다 — 첫 실행은 이 CTE 자체가
    --    컴파일되지 않아(is_incremental() 이 false) 드러나지 않는다.
    select
        check_name,
        source_name,
        severity as status,
        max(alerted_at) as last_alerted_at
    from {{ this }}
    group by 1, 2, 3

),
{% endif %}

to_send as (

    select a.*
    from alertable a
    {% if is_incremental() %}
    left join last_sent s
           on s.check_name  = a.check_name
          and s.source_name = a.source_name
          and s.status      = a.status
    where s.last_alerted_at is null
       or date_diff('hour', s.last_alerted_at, now()) >= {{ var('dq_alert_cooldown_hours') }}
    {% endif %}

)

select
    md5(check_name || ':' || source_name || ':' || status || ':' || check_date::VARCHAR) as alert_id,
    check_name,
    check_date,
    source_name,
    status                            as severity,
    observed_value,
    expected_value,
    detail,
    now()                             as alerted_at
from to_send
