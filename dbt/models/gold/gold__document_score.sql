{{ config(materialized='table') }}

-- 문서 랭킹. grain = doc_id. **멱등** — 언제 돌려도 같은 입력이면 같은 값이다.
-- (발송 여부 같은 상태는 여기 없다. 그건 gold__digest_dispatch 의 책임.)
--
--   score = affinity × recency_decay × (1 + engagement_boost)
--
-- ★ 세 신호의 스케일을 맞추는 방법과 근거
--
-- affinity  [0,1] 코사인. 실측 분포가 min 0.149 / median 0.391 / max 0.782 로
--           범위가 넓어 percentile 변환 없이 원값을 그대로 쓴다. 곱셈의 주축.
--
-- recency   [0,1] 지수 감쇠 0.5^(age/half_life). 소스마다 반감기가 다르다
--           (seed: arxiv 72h, hn 36h). HN 프론트페이지는 하루면 다들 봤지만
--           논문은 사흘 뒤에 읽어도 늦지 않다.
--
-- engagement [0,1] → (1+x) 로 [1,2] 배율. **곱이 아니라 (1+x)** 인 이유:
--           arXiv 는 인용수 API 가 없어 지표가 아예 없다. 그냥 곱하면 지표가
--           없다는 이유로 0점이 된다. (1+x) 면 지표 부재 = 배율 1.0 = 중립이다.
--           로그 스케일이라 2000점/일도 완만하게만 오른다 — 인기가 관련성을
--           압도하면 안 되므로 셋 중 가장 약한 신호로 설계했다.
--
-- ⚠️ 이 점수는 **같은 소스 안에서만** 비교 가능하다. HN 은 affinity median 이
--    0.479 인데 arXiv 는 0.371 이고(관심사가 엔지니어링 쪽이라 구조적으로 그렇다),
--    engagement 배율까지 HN 만 받는다. 전체 top-N 을 뽑으면 HN 이 전부 차지한다.
--    그래서 발송 선정은 소스별 쿼터로 한다 — dispatch 참고.

with metrics as (

    -- 일별 관측치에서 '하루에 몇 점 올랐나'를 만든다.
    select
        doc_id,
        metric_date,
        score,
        score - lag(score) over (partition by doc_id order by metric_date) as observed_delta
    from {{ ref('silver__document_metric_daily') }}

),

latest_metric as (

    select
        m.doc_id,
        m.score        as engagement_score,
        m.observed_delta,
        m.metric_date
    from metrics m
    qualify row_number() over (partition by m.doc_id order by m.metric_date desc) = 1

),

joined as (

    select
        d.doc_id,
        d.source,
        d.title,
        d.url,
        d.published_at,
        a.affinity,
        a.best_interest,
        a.best_interest_label,
        c.half_life_hours,
        lm.engagement_score,
        lm.observed_delta,
        lm.metric_date,
        date_diff('hour', d.published_at, now()) as age_hours
    from {{ ref('silver__document') }} d
    join {{ ref('gold__document_affinity') }} a using (doc_id)
    join {{ ref('source_config') }} c on c.source = d.source
    left join latest_metric lm on lm.doc_id = d.doc_id

),

velocity as (

    select
        *,
        -- 관측 델타가 있으면 그것, 첫 관측이라 없으면 '발행 이후 평균 상승률'로 추정한다.
        -- 둘 중 큰 값을 쓰는 이유: 방금 발견한 2000점 글(추정치가 큼)과
        -- 다시 불붙은 옛 글(관측 델타가 큼)을 둘 다 잡아야 한다.
        greatest(
            coalesce(observed_delta, 0),
            coalesce(
                engagement_score
                    / greatest(date_diff('day', published_at, coalesce(metric_date, now())), 1),
                0
            )
        ) as velocity_per_day
    from joined

)

select
    doc_id,
    source,
    title,
    url,
    published_at,
    best_interest,
    best_interest_label,

    affinity,
    age_hours,

    pow(0.5, age_hours::DOUBLE / half_life_hours)                  as recency_decay,

    least(
        1.0,
        ln(1 + greatest(velocity_per_day, 0))
            / ln(1 + {{ var('engagement_full_scale') }})
    )                                                              as engagement_boost,

    velocity_per_day,
    engagement_score,

    affinity
        * pow(0.5, age_hours::DOUBLE / half_life_hours)
        * (1 + least(
              1.0,
              ln(1 + greatest(velocity_per_day, 0))
                  / ln(1 + {{ var('engagement_full_scale') }})
          ))                                                       as score,

    now()                                                          as scored_at

from velocity
