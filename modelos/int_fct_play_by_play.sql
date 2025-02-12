with
pbp as (
    select * from {{ ref('stg_csv__play_by_play') }}
),
players as (
    select * from {{ ref('stg_csv__player_info') }}
),
teams as (
    select * from {{ ref('stg_csv__teams') }}
)
select
pbp.game_id,
pbp.time_in_period_time,
pbp.time_remaining,
pbp.sort_order,
pbp.period,
pbp.period_description,
pbp.event_code,
pbp.event_description,
pbp.event_reason,
pbp.event_secondary_reason,
pbp.penalty_type,
pbp.penalty_description,
pbp.penalty_duration,
p.full_name as penalty_by,
p2.full_name as penalty_drawn_by,
p3.full_name as penalty_served_by,
t.full_name as event_team,
t.team_id as event_team_id,
p4.full_name as scoring_by,
pbp.scoring_player_season_total,
p5.full_name as goalie_in_net,
pbp.away_score_at,
pbp.home_score_at,
p6.full_name as assist_1_by,
p7.full_name as assist_2_by,
pbp.assist_1_player_season_total,
p8.full_name as faceoff_losing_by,
p9.full_name as faceoff_winning_by,
pbp.event_x_coordinate,
pbp.event_y_coordinate,
pbp.event_zone_code,
p10.full_name as hitting_player,
p11.full_name as hittee_player,
p12.full_name as blocking_player,
p13.full_name as shooting_player,
p14.full_name as event_player,
pbp.shot_type,
pbp.away_sog_at,
pbp.home_sog_at,
pbp.assist_2_player_season_total,
pbp.home_defending_side,
pbp.details_highlight_clip_sharing_url,
pbp.sk
from pbp
left join  players p
on pbp.penalty_by_player_id = p.player_id
left join  players p2
on pbp.penalty_drawn_by_player_id= p2.player_id
left join  players p3
on pbp.penalty_served_by_player_id = p3.player_id
left join  players p4
on pbp.scoring_player_id = p4.player_id
left join  players p5
on pbp.goalie_in_net_player_id = p5.player_id
left join  players p6
on pbp.assist_1_player_id = p6.player_id
left join  players p7
on pbp.assist_2_player_id = p7.player_id
left join  players p8
on pbp.faceoff_losing_player_id = p8.player_id
left join  players p9
on pbp.faceoff_winning_player_id = p9.player_id
left join  players p10
on pbp.hitting_player_id = p10.player_id
left join  players p11
on pbp.hittee_player_id = p11.player_id
left join  players p12
on pbp.blocking_player_id = p12.player_id
left join  players p13
on pbp.shooting_player_id = p13.player_id
left join  players p14
on pbp.event_player_id = p14.player_id
-- left join teams t
-- on pbp.event_teamid = t.team_code
