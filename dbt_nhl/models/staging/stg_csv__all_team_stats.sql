with source as (
    select * from {{ source('nhl_raw', 'raw_all_team_stats') }}
),
renames as (
    SELECT
    CAST("teamId" AS int) AS team_id,
    "teamFullName" AS team_name,
    CAST("seasonId" AS int) AS season_id,
    CASE WHEN LOWER("gamesPlayed") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("gamesPlayed" AS float)) AS int) END AS games_played,
    CASE WHEN LOWER("goalsAgainst") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("goalsAgainst" AS float)) AS int) END AS goals_against,
    CASE WHEN LOWER("goalsAgainstPerGame") = 'nan' THEN NULL ELSE CAST("goalsAgainstPerGame" AS float) END AS goals_against_per_game,
    CASE WHEN LOWER("goalsFor") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("goalsFor" AS float)) AS int) END AS goals_for,
    CASE WHEN LOWER("goalsForPerGame") = 'nan' THEN NULL ELSE CAST("goalsForPerGame" AS float) END AS goals_for_per_game,
    CASE WHEN LOWER("losses") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("losses" AS float)) AS int) END AS losses,
    CASE WHEN LOWER("otLosses") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("otLosses" AS float)) AS int) END AS ot_losses,
    CASE WHEN LOWER("penaltyKillNetPct") = 'nan' THEN NULL ELSE CAST("penaltyKillNetPct" AS float) END AS penalty_kill_net_pctg,
    CASE WHEN LOWER("penaltyKillPct") = 'nan' THEN NULL ELSE CAST("penaltyKillPct" AS float) END AS penalty_kill_pctg,
    CASE WHEN LOWER("pointPct") = 'nan' THEN NULL ELSE CAST("pointPct" AS float) END AS point_pctg,
    CASE WHEN LOWER("points") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("points" AS float)) AS int) END AS points,
    CASE WHEN LOWER("powerPlayNetPct") = 'nan' THEN NULL ELSE CAST("powerPlayNetPct" AS float) END AS power_play_net_pctg,
    CASE WHEN LOWER("powerPlayPct") = 'nan' THEN NULL ELSE CAST("powerPlayPct" AS float) END AS power_play_pctg,
    CASE WHEN LOWER("regulationAndOtWins") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("regulationAndOtWins" AS float)) AS int) END AS regulation_and_ot_wins,
    CASE WHEN LOWER("shotsAgainstPerGame") = 'nan' THEN NULL ELSE CAST("shotsAgainstPerGame" AS float) END AS shots_against_per_game,
    CASE WHEN LOWER("shotsForPerGame") = 'nan' THEN NULL ELSE CAST("shotsForPerGame" AS float) END AS shots_for_per_game,
    CASE WHEN LOWER("ties") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("ties" AS float)) AS int) END AS ties,
    CASE WHEN LOWER("wins") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("wins" AS float)) AS int) END AS wins,
    CASE WHEN LOWER("winsInRegulation") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("winsInRegulation" AS float)) AS int) END AS wins_in_regulation,
    CASE WHEN LOWER("winsInShootout") = 'nan' THEN NULL ELSE CAST(ROUND(CAST("winsInShootout" AS float)) AS int) END AS wins_in_shootout,
    CASE WHEN LOWER("faceoffWinPct") = 'nan' THEN NULL ELSE CAST("faceoffWinPct" AS float) END AS faceoff_win_pctg
FROM source
)
select
{{ dbt_utils.generate_surrogate_key(['team_id','season_id']) }} as sk_teamid_seasonid, * from renames
