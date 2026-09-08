{{ config(materialized='view') }}

-- 문서 × 관심 토픽 코사인 유사도의 **최댓값**. grain = doc_id.
--
-- 왜 max 인가: 평균을 쓰면 6개 토픽 중 하나에 완벽히 맞는 문서가 나머지 5개와
-- 무관하다는 이유로 밀린다. 관심사는 OR 조건이지 AND 가 아니다.
--
-- best_interest 를 함께 남기는 이유는 다이제스트에 "왜 이걸 골랐는지"를
-- 적어주기 위해서다. 추천 이유가 없는 추천은 신뢰를 못 얻는다.

with scored as (

    select
        d.doc_id,
        i.interest_key,
        i.label,
        -- vec 은 FLOAT[] 가변 리스트라 고정 ARRAY 로 캐스팅해야 한다.
        -- 문서와 관심사가 같은 모델로 임베딩됐을 때만 비교가 의미를 가지므로
        -- model_name 일치를 조인 조건에 건다.
        array_cosine_similarity(
            e.vec::FLOAT[{{ var('embed_dim', 1024) }}],
            i.vec::FLOAT[{{ var('embed_dim', 1024) }}]
        ) as affinity
    from {{ ref('silver__document') }} d
    join {{ source('lake', 'ml__embedding') }} e
      on e.doc_id = d.doc_id
     and e.content_hash = d.content_hash          -- 최신 본문의 임베딩만
    cross join {{ source('lake', 'ml__interest_embedding') }} i
    where e.model_name = i.model_name

)

select
    doc_id,
    affinity,
    interest_key as best_interest,
    label        as best_interest_label
from scored
qualify row_number() over (partition by doc_id order by affinity desc) = 1
