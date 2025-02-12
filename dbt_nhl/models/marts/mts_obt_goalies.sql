with
goalies as (
    select * from {{ ref('int_fct_goalies')}}
)
select * from goalies
