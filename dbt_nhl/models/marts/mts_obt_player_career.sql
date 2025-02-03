with
player_stats as (
    select * from {{ ref('int_dime_player_current_stats') }}
),
player_info as (
    select * from {{ ref('int_dime_player') }}
)
select
ps.*,
pi.full_name,
pi.current_team_id
from player_stats ps
left join
player_info pi
on ps.player_id = pi.player_id
