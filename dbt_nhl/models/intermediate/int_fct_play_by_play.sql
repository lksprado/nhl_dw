with
pbp as (
    select * from {{ ref('stg_csv__play_by_play') }}
),
players as (
    select * from {{ ref('stg_csv__player_info') }}
)
-- ,game_log as (
--     select
--     game_id,
--     player_id,
--     player_team_trid
--     from {{ ref('stg_csv__all_game_log')}}
--     group by
--     game_id,
--     player_id,
--     player_team_trid
-- )
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
pbp.event_team_id,
pbp.penalty_type,
pbp.penalty_description,
pbp.penalty_duration
-- ,pbp.scoring_player_season_total,
pbp.away_score_at,
pbp.home_score_at,
-- pbp.assist_1_player_season_total,
-- pbp.assist_2_player_season_total,
pbp.event_x_coordinate,
pbp.event_y_coordinate,
pbp.event_zone_code,
pbp.shot_type,
pbp.away_sog_at,
pbp.home_sog_at,
pbp.home_defending_side
-- pbp.details_highlight_clip_sharing_url,
-- p.full_name as penalty_by,
-- p2.full_name as penalty_drawn_by,
-- p3.full_name as penalty_served_by,
-- p4.full_name as scoring_by,
-- p5.full_name as goalie_in_net,
-- p6.full_name as assist_1_by,
-- p7.full_name as assist_2_by,
-- p8.full_name as faceoff_losing_by,
-- p9.full_name as faceoff_winning_by,
-- p10.full_name as hitting_player,
-- p11.full_name as hittee_player,
-- p12.full_name as blocking_player,
-- p13.full_name as shooting_player,
-- p14.full_name as event_player
-- ,gl.player_team_trid as penalty_by_team_trid
-- gl2.player_team_trid as penalty_drawn_by_team_trid,
-- gl3.player_team_trid as penalty_served_by_team_trid,
-- gl4.player_team_trid as scoring_by_team_trid,
-- gl5.player_team_trid as goalie_in_net_team_trid,
-- gl6.player_team_trid as assist_1_by_team_trid,
-- gl7.player_team_trid as assist_2_by_team_trid,
-- gl8.player_team_trid as faceoff_losing_by_team_trid,
-- gl9.player_team_trid as faceoff_winning_by_team_trid,
-- gl10.player_team_trid as hitting_team_trid,
-- gl11.player_team_trid as hittee_player_team_trid,
-- gl11.player_team_trid as blocking_player_team_trid,
-- gl13.player_team_trid as shooting_player_team_trid,
-- gl14.player_team_trid as event_player_team_trid
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
-- left join  game_log gl
-- on pbp.penalty_by_player_id = gl.player_id and pbp.game_id = gl.game_id
-- left join game_log gl2
-- on pbp.penalty_drawn_by_player_id = gl2.player_id and pbp.game_id = gl2.game_id
-- left join game_log gl3
-- on pbp.penalty_served_by_player_id = gl3.player_id and pbp.game_id = gl3.game_id
-- left join game_log gl4
-- on pbp.scoring_player_id = gl4.player_id and pbp.game_id = gl4.game_id
-- left join game_log gl5
-- on pbp.goalie_in_net_player_id = gl5.player_id and pbp.game_id = gl5.game_id
-- left join game_log gl6
-- on pbp.assist_1_player_id = gl6.player_id and pbp.game_id = gl6.game_id
-- left join game_log gl7
-- on pbp.assist_2_player_id = gl7.player_id and pbp.game_id = gl7.game_id
-- left join game_log gl8
-- on pbp.faceoff_losing_player_id = gl8.player_id and pbp.game_id = gl8.game_id
-- left join game_log gl9
-- on pbp.faceoff_winning_player_id = gl9.player_id and pbp.game_id = gl9.game_id
-- left join game_log gl10
-- on pbp.hitting_player_id = gl10.player_id and pbp.game_id = gl10.game_id
-- left join game_log gl11
-- on pbp.hittee_player_id = gl11.player_id and pbp.game_id = gl11.game_id
-- left join game_log gl12
-- on pbp.blocking_player_id = gl12.player_id and pbp.game_id = gl12.game_id
-- left join game_log gl13
-- on pbp.shooting_player_id = gl13.player_id and pbp.game_id = gl13.game_id
-- left join game_log gl14
-- on pbp.event_player_id = gl14.player_id and pbp.game_id = gl14.game_id
