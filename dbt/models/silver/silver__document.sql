{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'doc_id',
  )
}}

-- 소스별 staging 을 하나의 문서 테이블로 합친다. grain = doc_id.
--
-- 이 모델의 존재 이유:
--   1. published_at 을 payload JSON 에서 꺼내 1급 TIMESTAMPTZ 컬럼으로 승격.
--      전신 프로젝트는 이 값이 meta JSON 문자열에 묻혀 있어 recency 랭킹 자체가
--      불가능했다. 랭킹의 축이 될 값은 컬럼이어야 한다.
--   2. 소스가 늘어도 소비자(gold·임베딩)는 이 한 테이블만 보면 되게 한다.
--      새 소스 추가 = staging 파일 하나 + 여기 union 한 줄.
--
-- ⚠️ incremental 필터는 published_at 이 아니라 _source_fetched_at(수집 시각) 기준이다.
--    이벤트 시각으로 자르면, 3일 전 발행된 논문이 오늘 인덱싱됐을 때 필터에 걸려
--    영원히 유실된다. 수집 시각으로 자르고 이벤트 시각은 데이터로만 쓴다.
--    재처리되는 행은 merge 가 멱등하게 흡수한다.

with unioned as (

    select * from {{ ref('stg_arxiv__document') }}
    union all
    select * from {{ ref('stg_hackernews__document') }}

)

select
    doc_id,
    source,
    source_name,
    native_id,
    title,
    body,
    url,
    version,
    authors,
    categories,
    published_at,
    source_updated_at,

    -- 임베딩 대상 텍스트의 해시. payload_hash 와 일부러 다르게 둔다.
    -- HN 점수만 바뀌면 payload_hash 는 변하지만 본문은 그대로이므로
    -- 재임베딩할 이유가 없다. Phase 4 의 백로그 안티조인이 이 컬럼을 grain 에 쓴다.
    md5(coalesce(title, '') || E'\n' || coalesce(body, ''))  as content_hash,

    payload_hash,
    _source_fetched_at

from unioned

{% if is_incremental() %}
where _source_fetched_at > (
    select coalesce(max(_source_fetched_at), '1970-01-01'::timestamptz)
    from {{ this }}
) - interval {{ var('silver_lookback_days') }} day
{% endif %}
