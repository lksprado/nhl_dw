with
games as (
    select * from {{ ref('int_fct_game_info') }}
),
teams as (
    select * from {{ ref('int_dime_teams') }}
)
select
g.game_id,
g.season_id,
g.game_date,
g.start_time_date_eastern,
g.start_time_date_gmt_minus_3,
t2.full_name as home_team_name,
t.full_name as away_team_name,
g.game_type,
g.game_number,
g.number_of_periods,
g.away_score,
g.home_score,
g.home_sog,
g.away_sog,
g.referee_1,
g.referee_2,
g.referee_3,
g.home_score_period_1,
g.home_score_period_2,
g.home_score_period_3,
g.home_score_period_4,
g.home_score_period_5,
g.away_score_period_1,
g.away_score_period_2,
g.away_score_period_3,
g.away_score_period_4,
g.away_score_period_5,
g.home_shots_period_1,
g.home_shots_period_2,
g.home_shots_period_3,
g.home_shots_period_4,
g.home_shots_period_5,
g.away_shots_period_1,
g.away_shots_period_2,
g.away_shots_period_3,
g.away_shots_period_4,
g.away_shots_period_5,
g.home_powerplay,
g.away_powerplay,
g.home_powerplay_pctg,
g.away_powerplay_pctg,
g.home_faceoff_winning_pctg,
g.away_faceoff_winning_pctg,
g.home_hits,
g.away_hits,
g.home_giveaways,
g.away_giveaways,
g.home_takeaways,
g.away_takeaways,
g.away_penalty_minutes,
g.home_penalty_minutes,
g.home_blocked_shots,
g.away_blocked_shots
from games g
left join teams t
on g.away_team_id = t.team_code
left join teams t2
on g.home_team_id = t2.team_code
