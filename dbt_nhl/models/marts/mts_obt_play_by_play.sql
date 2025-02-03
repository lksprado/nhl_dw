with
pbp as (
    select * from {{ ref('int_fct_play_by_play') }}
)
select * from pbp
