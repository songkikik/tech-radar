{{ config(materialized='ephemeral') }}

-- arXiv payload → 공통 문서 스키마.
-- ephemeral 인 이유: 이 모델 자체를 조회할 일이 없고, silver 의 incremental 필터가
-- 이 CTE 안으로 밀려들어가야 스캔이 줄기 때문이다.

with latest as (

    -- 같은 논문의 payload 가 여러 번 바뀌었으면 가장 최근 수집분만.
    -- 변경 이력은 bronze 에 그대로 남아 있다.
    --
    -- ⚠️ 파티션 키가 native_id 인 것이 중요하다(source_name 을 넣으면 안 된다).
    --    arXiv 는 같은 논문을 여러 카테고리에 교차 등재(cross-listing)한다.
    --    예: 2609.03760 이 cs.RO 와 cs.SE 양쪽에 존재.
    --    source_name 을 파티션에 넣으면 카테고리마다 1건씩 남는데, doc_id 는
    --    'arxiv:' || native_id 라 카테고리가 달라도 같은 값이 된다 → doc_id 중복.
    --
    --    카테고리가 cs.AI 하나뿐일 때는 두 파티션 키가 같은 결과라 드러나지 않다가,
    --    카테고리를 7개로 늘리자 20건이 중복되며 unique 테스트가 즉시 잡아냈다.
    --    파티션 키의 grain 은 최종 식별자(doc_id)의 grain 과 일치해야 한다.
    select *
    from {{ source('lake', 'bronze__raw_item') }}
    where source_name like 'arxiv:%'
    qualify row_number() over (
        partition by native_id
        order by fetched_at desc, event_ts desc
    ) = 1

)

select
    'arxiv:' || native_id                                   as doc_id,
    'arxiv'                                                 as source,
    source_name,
    native_id,

    json_extract_string(payload, '$.title')                 as title,
    json_extract_string(payload, '$.summary')               as body,
    json_extract_string(payload, '$.abs_url')               as url,
    json_extract_string(payload, '$.version')               as version,

    from_json(json_extract(payload, '$.authors'),    '["VARCHAR"]') as authors,
    from_json(json_extract(payload, '$.categories'), '["VARCHAR"]') as categories,

    try_cast(json_extract_string(payload, '$.published') as timestamptz) as published_at,
    try_cast(json_extract_string(payload, '$.updated')   as timestamptz) as source_updated_at,

    payload_hash,
    fetched_at                                              as _source_fetched_at

from latest
