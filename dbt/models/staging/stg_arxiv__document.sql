{{ config(materialized='ephemeral') }}

-- arXiv payload → 공통 문서 스키마.
-- ephemeral 인 이유: 이 모델 자체를 조회할 일이 없고, silver 의 incremental 필터가
-- 이 CTE 안으로 밀려들어가야 스캔이 줄기 때문이다.

with latest as (

    -- 같은 논문의 payload 가 여러 번 바뀌었으면 가장 최근 수집분만.
    -- 변경 이력은 bronze 에 그대로 남아 있다.
    select *
    from {{ source('lake', 'bronze__raw_item') }}
    where source_name like 'arxiv:%'
    qualify row_number() over (
        partition by source_name, native_id
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
