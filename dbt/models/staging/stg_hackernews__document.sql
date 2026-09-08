{{ config(materialized='ephemeral') }}

-- HN payload → 공통 문서 스키마.
--
-- arXiv 와 두 곳이 다르다:
--   time    unix epoch 정수 → to_timestamp 필요 (arXiv 는 ISO 문자열)
--   url     텍스트 글(Ask HN 등)은 url 이 없다 → HN 항목 페이지로 폴백.
--           다이제스트에 링크 없는 항목이 나가면 안 되므로 여기서 보장한다.

with latest as (

    -- 파티션 키는 native_id 다(source_name 을 넣으면 안 된다). 지금은 top 리스팅
    -- 하나만 수집해서 결과가 같지만, best/new 를 추가하면 같은 글이 두 리스팅에
    -- 동시에 올라 doc_id 가 중복된다 — arXiv 교차 등재에서 실제로 터진 것과 같은 버그다.
    select *
    from {{ source('lake', 'bronze__raw_item') }}
    where source_name like 'hackernews:%'
    qualify row_number() over (
        partition by native_id
        order by fetched_at desc, event_ts desc
    ) = 1

)

select
    'hackernews:' || native_id                              as doc_id,
    'hackernews'                                            as source,
    source_name,
    native_id,

    json_extract_string(payload, '$.title')                 as title,
    json_extract_string(payload, '$.text')                  as body,
    coalesce(
        json_extract_string(payload, '$.url'),
        'https://news.ycombinator.com/item?id=' || native_id
    )                                                       as url,
    cast(null as varchar)                                   as version,

    -- 작성자 1명을 arXiv 의 authors 배열과 같은 모양으로 맞춘다.
    [json_extract_string(payload, '$.by')]                  as authors,
    cast([] as varchar[])                                   as categories,

    to_timestamp(try_cast(json_extract_string(payload, '$.time') as bigint)) as published_at,
    cast(null as timestamptz)                               as source_updated_at,

    payload_hash,
    fetched_at                                              as _source_fetched_at

from latest
