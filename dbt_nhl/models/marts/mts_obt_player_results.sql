with
player_results_per_game as (
    select * from {{ ref('int_fct_player_games')}}
)
select * from player_results_per_game
