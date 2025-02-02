with
stats as (
    select * from {{ ref('int_fct_team_stats') }}
),
teams as (
    select * from {{ ref('int_dime_teams') }}
)
select
s.season_id,
s.team_id,
t.full_name,
t.is_active,
s.games_played,
s.wins,
s.losses,
s.ot_losses,
s.wins_in_regulation,
s.wins_in_shootout,
s.points,
s.point_pctg,
s.goals_for,
s.goals_for_per_game,
s.goals_against,
s.goals_against_per_game,
s.penalty_kill_net_pctg,
s.penalty_kill_pctg,
s.powerplay_net_pctg,
s.powerplay_pctg,
s.regulation_and_ot_wins,
s.shots_against_per_game,
s.shots_for_per_game,
s.faceoff_win_pctg
from stats s
left join teams t
on s.team_id = t.team_code
