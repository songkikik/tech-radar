{{ config(materialized='view') }}

-- 아직 임베딩되지 않은 문서 목록. Python 의 embed 잡이 이 뷰를 읽어 인코딩한다.
--
-- ★ 왜 워터마크가 아니라 안티조인인가
--
-- "마지막으로 임베딩한 시각" 같은 커서를 두면, 그 시각 이후에 *새로 들어온* 문서는
-- 잡지만 *본문이 수정된* 기존 문서는 놓친다. arXiv 개정판(v2)이 정확히 그 경우다.
-- 상태를 들고 있는데 그 상태가 틀리는 것보다, 상태를 아예 두지 않는 쪽이 견고하다.
--
-- content_hash 를 조인 조건에 넣었으므로 본문이 바뀌면 매칭이 깨져 자동으로
-- 백로그에 다시 나타난다. 무효화(invalidation) 로직이 따로 필요 없다.
--
-- model_name 도 조인 조건이다. 모델을 바꾸면 그 모델 이름으로는 임베딩이 하나도
-- 없으므로 전체가 백로그가 된다 = 전량 삭제 후 재계산이 아니라 백필. 옛 모델
-- 임베딩은 그대로 남아 있어 A/B 후 롤백이 가능하다.
--
-- ⚠️ 이 뷰는 '해야 할 일'만 알려주고 상태를 갖지 않는다. 런당 캡(embed_batch_cap)에
--    걸려 남은 분량은 다음 실행이 알아서 집어간다 = self-healing.

select
    d.doc_id,
    d.source,
    d.title,
    d.body,
    d.content_hash,
    d.published_at

from {{ ref('silver__document') }} d

left join {{ source('lake', 'ml__embedding') }} e
       on e.doc_id       = d.doc_id
      and e.content_hash = d.content_hash
      and e.model_name   = '{{ var("embed_model") }}'

where e.doc_id is null

-- 최신 문서부터. 캡에 걸려 잘리더라도 다이제스트에 쓸 것부터 준비된다.
order by d.published_at desc
limit {{ var('embed_batch_cap') }}
