{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['doc_id', 'metric_date'],
  )
}}

-- 문서의 참여 지표를 일자별로 보존한다. grain = doc_id × metric_date.
--
-- 왜 필요한가: silver__document 는 문서당 1행이라 최신 점수만 남는다. 그런데
-- 트렌드 신호로 쓸 값은 "지금 몇 점"이 아니라 "얼마나 빠르게 오르고 있나"다.
-- HN 글이 3일 뒤에 점수가 폭발하는 일이 흔한데, 최신값만 보면 그 속도를 못 잡는다.
-- 델타 계산 자체는 gold 의 책임이고, 여기서는 원재료(일별 관측치)만 쌓는다.
--
-- ⚠️ 플랜은 append 전략이었으나 merge 로 바꿨다. append 는 dbt 를 다시 돌릴 때마다
--    같은 날짜의 행이 중복으로 쌓인다. 개발 중 dbt build 를 수시로 돌리는 이상
--    (doc_id, metric_date) 멱등이 아니면 지표가 조용히 오염된다.
--
-- 현재는 HN 만 지표를 제공한다. arXiv 는 인용수 API 가 없고, GitHub 은 Phase 8 에서
-- stars 를 같은 모양으로 얹으면 된다.

with observations as (

    select
        'hackernews:' || native_id                                        as doc_id,
        fetched_at,
        try_cast(json_extract_string(payload, '$.score')       as integer) as score,
        try_cast(json_extract_string(payload, '$.descendants') as integer) as comments
    from {{ source('lake', 'bronze__raw_item') }}
    where source_name like 'hackernews:%'

    {% if is_incremental() %}
      and fetched_at > (
          select coalesce(max(observed_at), '1970-01-01'::timestamptz)
          from {{ this }}
      ) - interval {{ var('silver_lookback_days') }} day
    {% endif %}

)

select
    doc_id,
    fetched_at::date            as metric_date,

    -- 하루에 여러 번 수집했다면 그날의 '마지막 관측치'를 쓴다.
    -- max() 가 아닌 이유: 점수는 플래깅으로 내려갈 수도 있어서, 최댓값은
    -- 그날의 실제 상태가 아니라 그날의 최고점이 된다. 델타를 계산할 때 왜곡된다.
    arg_max(score,    fetched_at) as score,
    arg_max(comments, fetched_at) as comments,
    max(fetched_at)               as observed_at

from observations
group by 1, 2
