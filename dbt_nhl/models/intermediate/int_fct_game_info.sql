with
game_info as (
    select * from {{ ref('stg_csv__all_game_info')}}
),
dime_teams as (
    select * from {{ ref('int_dime_teams')}}
),
game_sogs as (
    select * from {{ ref('stg_csv__game_info_sog')}}
)
select
gi.game_id,
gi.season_id,
gi.game_date,
gi.start_time_date_eastern,
gi.start_time_date_gmt_minus_3,
gi.game_type,
gi.game_number,
gi.number_of_periods,
dt.team_id as home_team_id,
case when dt.full_name is null then gs.home_team_full_name else dt.full_name  end as home_team_name,
dt2.team_id as visiting_team_id,
case when dt2.full_name is null then gs.away_team_full_name else dt2.full_name end as visiting_team_name,
gi.visiting_score,
gi.home_score,
gs.home_team_sog,
gs.away_team_sog
from game_info gi
left join dime_teams dt
on gi.home_team_id = dt.team_code
left join dime_teams dt2
on gi.visiting_team_id = dt2.team_code
left join game_sogs gs
on gi.game_id = gs.game_id
