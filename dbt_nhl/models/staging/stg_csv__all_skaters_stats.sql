with source as (
    select * from {{ source('nhl_raw', 'raw_all_skaters_stats') }}
),
renamed as (
    select
    concat("playerid","seasonid") as pk,
    cast("playerid" as int) as player_id,
    cast("seasonid" as int) as season_id,
    "skaterfullname" as full_name,
    "lastname" as last_name,
    "shootscatches" as shoots_or_catches,
    "positioncode" as position_code,
    {{ string_to_int('"gamesplayed"') }}  as games_played,
    {{ string_to_int('"goals"') }} as goals,
    {{ string_to_int('"assists"') }} as assists,
    {{ string_to_int('"points"') }} as points,
    {{ string_to_int('"evgoals"') }} as ev_goals,
    {{ string_to_int('"evpoints"') }} as ev_points,
    {{ string_to_int('"otgoals"') }} as ot_goals,
    {{ string_to_int('"ppgoals"') }} as powerplay_goals,
    {{ string_to_int('"pppoints"') }} as powerplay_points,
    {{ string_to_int('"shgoals"') }} as shorthanded_goals,
    {{ string_to_int('"shpoints"') }} as shorthanded_points,
    {{ string_to_int('"shots"') }} as shots,
    {{ string_to_int('"gamewinninggoals"') }} as game_winning_goals,
    {{ string_to_int('"plusminus"') }} as plus_minus,
    {{ string_to_float('"timeonicepergame"') }}/60 as time_on_ice_per_game_minutes,
    {{ string_to_int('"penaltyminutes"') }} as penalty_minutes,
    {{ string_to_float('"faceoffwinpct"') }} as faceoff_win_pctg,
    {{ string_to_float('"pointspergame"') }} as points_per_game,
    {{ string_to_float('"shootingpct"') }} as shooting_pctg
    from source
),
grouping as (
    select
    pk,
    player_id,
    season_id,
    full_name,
    last_name,
    shoots_or_catches,
    position_code,
    sum(games_played) as games_played,
    sum(goals) as goals,
    sum(assists) as assists,
    sum(points) as points,
    sum(ev_goals) as ev_goals,
    sum(ev_points) as ev_points,
    sum(ot_goals) as ot_goals,
    sum(powerplay_goals) as powerplay_goals,
    sum(powerplay_points) as powerplay_points,
    sum(shorthanded_goals) as shorthanded_goals,
    sum(shorthanded_points) as shorthanded_points,
    sum(shots) as shots,
    sum(game_winning_goals) as game_winning_goals,
    sum(plus_minus) as plus_minus,
    (sum(time_on_ice_per_game_minutes)*sum(games_played))::int as time_on_ice_minutes,
    sum(penalty_minutes) as penalty_minutes,
    round((sum(faceoff_win_pctg)/2)::NUMERIC,3) as faceoff_win_pctg,
    CASE
        WHEN sum(games_played) = 0 THEN NULL
        ELSE ROUND((sum(points)::NUMERIC / sum(games_played)), 3)
    END AS points_per_game,
    CASE
        WHEN sum(shots) = 0 THEN NULL
        ELSE ROUND((sum(goals)::NUMERIC / sum(shots)), 3)
    END AS shooting_pctg
    from renamed
    group by
    pk,
    player_id,
    season_id,
    full_name,
    last_name,
    shoots_or_catches,
    position_code
)
select * from grouping
