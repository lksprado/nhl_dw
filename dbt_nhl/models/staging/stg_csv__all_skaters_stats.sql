with source as (
    select * from {{ source('nhl_raw', 'raw_all_skaters_stats') }}
),
renamed as (
    select
    concat("playerId","seasonId") as pk,
    cast("playerId" AS int) AS player_id,
    CAST("seasonId" AS int) AS season_id,
    "skaterFullName" AS full_name,
    "lastName" AS last_name,
    "shootsCatches" AS shoots_or_catches,
    "positionCode" AS position_code,
    {{ string_to_int('"gamesPlayed"') }}  AS games_played,
    {{ string_to_int('"goals"') }} as goals,
    {{ string_to_int('"assists"') }} as assists,
    {{ string_to_int('"points"') }} as points,
    {{ string_to_int('"evGoals"') }} as ev_goals,
    {{ string_to_int('"evPoints"') }} as ev_points,
    {{ string_to_int('"otGoals"') }} as ot_goals,
    {{ string_to_int('"ppGoals"') }} as powerplay_goals,
    {{ string_to_int('"ppPoints"') }} as powerplay_points,
    {{ string_to_int('"shGoals"') }} as shorthanded_goals,
    {{ string_to_int('"shPoints"') }} as shorthanded_points,
    {{ string_to_int('"shots"') }} as shots,
    {{ string_to_int('"gameWinningGoals"') }} as game_winning_goals,
    {{ string_to_int('"plusMinus"') }} as plus_minus,
    {{ string_to_float('"timeOnIcePerGame"') }} as time_on_ice_per_game,
    {{ string_to_int('"penaltyMinutes"') }} as penalty_minutes,
    {{ string_to_float('"faceoffWinPct"') }} as faceoff_win_pctg,
    {{ string_to_float('"pointsPerGame"') }} as points_per_game,
    {{ string_to_float('"shootingPct"') }} as shooting_pctg
    from source
),
dup as (
    {{ dbt_utils.deduplicate(
        relation='renamed',
        partition_by='pk',
        order_by= 'season_id'
        )
    }}
)
select * from dup
