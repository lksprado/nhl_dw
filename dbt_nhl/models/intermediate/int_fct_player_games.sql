with
players as (
    select * from {{ ref('stg_csv__all_boxscore_players') }}
),
game_info as (
    select * from {{ ref('stg_csv__all_game_info') }}
),
player_info as (
    select
    player_id,
    birth_date,
    full_name,
    is_active,
    height_inches,
    weight_pounds,
    height_centimeters,
    weight_kg,
    birth_country,
    birth_city,
    birth_state
    from {{ ref('stg_csv__player_info') }}
)
select
p.game_id,
gi.season_id,
gi.game_date,
gi.start_time_date_eastern,
gi.start_time_date_gmt_minus_3,
gi.game_type,
gi.game_number,
p.player_id,
pi.birth_date,
pi.full_name,
EXTRACT(year FROM AGE(gi.game_date, pi.birth_date)) AS player_age_at,
pi.is_active,
pi.height_inches,
pi.weight_pounds,
pi.height_centimeters,
pi.weight_kg,
pi.birth_country,
pi.birth_city,
pi.birth_state,
p.jersey_number,
p.position,
p.goals,
p.assists,
p.points,
p.plus_minus,
p.penalty_minutes,
p.hits,
p.powerplay_goals,
p.sog,
p.faceoff_winning_pctg,
p.blocked_shots,
p.shifts,
p.giveaways,
p.takeways,
p.even_strenght_goals_against,
p.powerplay_goals_against,
p.shorthanded_goals_against,
p.goals_against,
p.time_on_ice_minutes,
p.starter,
p.decision,
p.shots_against,
p.saves,
p.save_pctg
from players p
left join game_info gi
on p.game_id = gi.game_id
left join player_info pi
on p.player_id = pi.player_id
