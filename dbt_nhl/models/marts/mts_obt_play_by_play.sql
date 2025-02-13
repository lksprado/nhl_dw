with
play_by_play as (
    select * from {{ ref('int_fct_play_by_play')}}
)
select * from play_by_play
