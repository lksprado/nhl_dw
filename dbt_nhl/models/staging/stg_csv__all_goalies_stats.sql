with source as (
    select * from {{ source ('nhl_raw', 'raw_all_goalies_stats')}}
),
renamed as (
    select
    CASE WHEN LOWER("playerId") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("playerId" AS float)) AS int) END AS player_id,
    "lastName" AS last_name,
    "goalieFullName" AS full_name,
    cast("assists" AS int) AS assists,
    "teamAbbrevs" AS team_abbrev,
    CAST("seasonId" AS int) AS season_id,
    CASE WHEN LOWER("gamesPlayed") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("gamesPlayed" AS float)) AS int) END AS games_played,
    CASE WHEN LOWER("goals") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("goals" AS float)) AS int) END AS goals,
    CASE WHEN LOWER("penaltyMinutes") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("penaltyMinutes" AS float)) AS int) END AS penalty_minutes,
    CASE WHEN LOWER("gamesStarted") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("gamesStarted" AS float)) AS int) END AS games_started,
    CASE WHEN LOWER("goalsAgainst") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("goalsAgainst" AS float)) AS int) END AS goals_against,
    CASE WHEN LOWER("goalsAgainstAverage") = 'nan' THEN NULL ELSE CAST("goalsAgainstAverage" AS float) END AS goals_against_average,
    CASE WHEN LOWER("losses") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("losses" AS float)) AS int) END AS losses,
    CASE WHEN LOWER("points") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("points" AS float)) AS int) END AS points,
    CASE WHEN LOWER("savePct") = 'nan' THEN NULL ELSE CAST("savePct" AS float) END AS save_pctg,
    CASE WHEN LOWER("saves") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("saves" AS float)) AS int) END AS saves,
    "shootsCatches" AS shoots_or_catches,
    CASE WHEN LOWER("shutouts") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("shutouts" AS float)) AS int) END AS shutouts,
    CASE WHEN LOWER("ties") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("ties" AS float)) AS int) END AS ties,
    CASE WHEN LOWER("timeOnIce") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("timeOnIce" AS float)) AS int) END AS time_on_ice,
    CASE WHEN LOWER("total") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("total" AS float)) AS int) END AS total,
	CASE WHEN LOWER("wins") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("wins" AS float)) AS int) END AS wins
    from source
)
select * from renamed
