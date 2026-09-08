{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'digest_date',
  )
}}

-- 오늘 보낼 목록. grain = digest_date × doc_id.
--
-- 감지(랭킹)와 발송(선정)을 분리한 구조다. gold__document_score 는 멱등이라
-- 언제 돌려도 같은 값이고, 여기서만 "누구에게 언제 보냈나"라는 상태가 생긴다.
-- price-integrity 의 anomaly_event / notification_dispatch 분리와 같은 형태.
--
-- ★ '이미 보낸 문서' 판정을 발송 성공 기준으로 한다
--   선정 기록만으로 제외하면, Slack 전송이 실패한 문서는 영영 못 나간다.
--   그것도 조용히. ops__digest_delivery(status='sent')를 봐야 실패분이
--   다음날 후보로 돌아온다.
--
-- ★ 소스별 쿼터를 쓰는 이유 (전체 top-N 이 아니라)
--   실측: affinity median 이 HN 0.479 vs arXiv 0.371 이다. 관심사가 엔지니어링
--   쪽이라 연구 논문이 구조적으로 낮게 나오고, engagement 배율까지 HN 만 받는다.
--   전체 top-8 을 뽑으면 8건 전부 HN 이 된다. 쿼터는 다양성을 확률이 아니라
--   보장으로 만든다.
--
-- delete+insert 인 이유: 같은 날 다시 돌리면 그날 선정을 통째로 재계산한다.
-- append 면 중복이 쌓이고, merge 면 빠졌어야 할 옛 선정이 남는다.

with already_sent as (

    select distinct doc_id
    from {{ source('lake', 'ops__digest_delivery') }}
    where status = 'sent'

),

candidates as (

    select s.*
    from {{ ref('gold__document_score') }} s
    left join already_sent a on a.doc_id = s.doc_id
    where a.doc_id is null
      and s.affinity >= {{ var('digest_affinity_floor') }}
      and s.title is not null

),

ranked as (

    select
        *,
        row_number() over (partition by source order by score desc) as rank_in_source
    from candidates

)

select
    current_date::VARCHAR || ':' || r.doc_id  as dispatch_id,
    current_date                              as digest_date,
    r.doc_id,
    r.source,
    r.title,
    r.url,
    r.published_at,
    r.best_interest_label,
    r.affinity,
    r.recency_decay,
    r.engagement_boost,
    r.score,
    r.rank_in_source,
    now()                                     as selected_at

from ranked r
join {{ ref('source_config') }} c on c.source = r.source
where r.rank_in_source <= c.digest_quota
order by r.source, r.rank_in_source
