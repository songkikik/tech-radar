-- grain(doc_id × metric_date) 유일성.
--
-- dbt_utils.unique_combination_of_columns 를 쓰지 않는 이유: 패키지 하나를
-- 이 테스트 하나 때문에 끌어오는 건 과하다. 복합키 유일성은 SQL 4줄이면 된다.
--
-- 이게 깨지면 merge 의 unique_key 설정이 잘못됐거나 append 로 되돌아갔다는 뜻이다.
select
    doc_id,
    metric_date,
    count(*) as n
from {{ ref('silver__document_metric_daily') }}
group by 1, 2
having count(*) > 1
