{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'doc_id',
  )
}}

-- bronze 원본 payload 를 문서 1건 = 1행으로 정규화한다.
--
-- 이 모델의 존재 이유 두 가지:
--   1. published_at 을 payload JSON 에서 꺼내 1급 TIMESTAMPTZ 컬럼으로 승격.
--      전신 프로젝트는 이 값이 meta JSON 문자열에 묻혀 있어 recency 랭킹 자체가
--      불가능했다. 랭킹의 축이 될 값은 컬럼이어야 한다.
--   2. bronze 의 이력(같은 문서의 payload 변경분)을 최신 1건으로 접는다.
--
-- ⚠️ incremental 필터는 published_at 이 아니라 fetched_at(수집 시각) 기준이다.
--    이벤트 시각으로 자르면, 3일 전 발행된 논문이 오늘 인덱싱됐을 때 필터에 걸려
--    영원히 유실된다. 수집 시각으로 자르고 이벤트 시각은 데이터로만 쓴다.
--    재처리되는 행은 merge 가 멱등하게 흡수한다.

with source_rows as (

    select *
    from {{ source('lake', 'bronze__raw_item') }}

    {% if is_incremental() %}
    where fetched_at > (
        select coalesce(max(_source_fetched_at), '1970-01-01'::timestamptz)
        from {{ this }}
    ) - interval {{ var('silver_lookback_days') }} day
    {% endif %}

),

latest_per_doc as (

    -- 같은 문서의 payload 가 여러 번 바뀌었으면 가장 최근 수집분만 남긴다.
    -- 변경 이력 자체는 bronze 에 그대로 보존돼 있다.
    select *
    from source_rows
    qualify row_number() over (
        partition by source_name, native_id
        order by fetched_at desc, event_ts desc
    ) = 1

),

parsed as (

    select
        split_part(source_name, ':', 1) || ':' || native_id     as doc_id,
        split_part(source_name, ':', 1)                         as source,
        source_name,
        native_id,

        json_extract_string(payload, '$.title')                 as title,
        json_extract_string(payload, '$.summary')               as body,
        json_extract_string(payload, '$.abs_url')               as url,
        json_extract_string(payload, '$.version')               as version,
        from_json(json_extract(payload, '$.authors'),    '["VARCHAR"]') as authors,
        from_json(json_extract(payload, '$.categories'), '["VARCHAR"]') as categories,

        -- 1급 승격. 여기가 이 모델의 핵심.
        try_cast(json_extract_string(payload, '$.published') as timestamptz) as published_at,
        try_cast(json_extract_string(payload, '$.updated')   as timestamptz) as source_updated_at,

        payload_hash,
        fetched_at                                              as _source_fetched_at

    from latest_per_doc

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
    md5(coalesce(title, '') || E'\n' || coalesce(body, ''))     as content_hash,

    payload_hash,
    _source_fetched_at

from parsed
where doc_id is not null
