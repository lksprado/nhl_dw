with source as (
    select * from {{ source('nhl_raw', 'raw_all_skaters_stats') }}
),
renamed as (
    select
    CASE WHEN LOWER("playerId") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("playerId" AS float)) AS int) END AS player_id,
    "positionCode" AS position_code,
    "lastName" AS last_name,
    "skaterFullName" AS full_name,
    cast("assists" AS int) AS assists,
    "teamAbbrevs" AS team_abbrev,
    CAST("seasonId" AS int) AS season_id,
    CASE WHEN LOWER("evGoals") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("evGoals" AS float)) AS int) END AS ev_goals,
    CASE WHEN LOWER("evPoints") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("evPoints" AS float)) AS int) END AS ev_points,
    CASE WHEN LOWER("faceoffWinPct") = 'nan' THEN NULL ELSE CAST("faceoffWinPct" AS float) END AS faceoff_win_pctg,
    CASE WHEN LOWER("gameWinningGoals") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("gameWinningGoals" AS float)) AS int) END AS game_winning_goals,
    CASE WHEN LOWER("gamesPlayed") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("gamesPlayed" AS float)) AS int) END AS games_played,
    CASE WHEN LOWER("goals") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("goals" AS float)) AS int) END AS goals,
    CASE WHEN LOWER("otGoals") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("otGoals" AS float)) AS int) END AS ot_goals,
    CASE WHEN LOWER("penaltyMinutes") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("penaltyMinutes" AS float)) AS int) END AS penalty_minutes,
    CASE WHEN LOWER("plusMinus") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("plusMinus" AS float)) AS int) END AS plus_minus,
    CASE WHEN LOWER("points") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("points" AS float)) AS int) END AS points,
    CASE WHEN LOWER("pointsPerGame") = 'nan' THEN NULL ELSE CAST("pointsPerGame" AS float) END AS points_per_game,
    CASE WHEN LOWER("ppGoals") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("ppGoals" AS float)) AS int) END AS powerplay_goals,
    CASE WHEN LOWER("ppPoints") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("ppPoints" AS float)) AS int) END AS powerplay_points,
    CASE WHEN LOWER("shGoals") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("shGoals" AS float)) AS int) END AS shorthanded_goals,
    CASE WHEN LOWER("shPoints") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("shPoints" AS float)) AS int) END AS shorthanded_points,
    CASE WHEN LOWER("shootingPct") = 'nan' THEN NULL ELSE CAST("shootingPct" AS float) END AS shooting_pctg,
    CASE WHEN LOWER("shots") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("shots" AS float)) AS int) END AS shots,
    CASE WHEN LOWER("timeOnIcePerGame") = 'nan' THEN NULL ELSE CAST("timeOnIcePerGame" AS float) END AS time_on_ice_per_game,
    CASE WHEN LOWER("total") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("total" AS float)) AS int) END AS total,
    concat("playerId","seasonId") as sk
    from source
),
dup as (
    {{ dbt_utils.deduplicate(
        relation='renamed',
        partition_by='sk',
        order_by= 'season_id'
        )
    }}
)
select * from dup
