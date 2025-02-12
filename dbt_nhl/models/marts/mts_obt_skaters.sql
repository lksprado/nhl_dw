with
skaters as (
    select * from {{ ref('int_fct_skaters')}}
)
select * from skaters
