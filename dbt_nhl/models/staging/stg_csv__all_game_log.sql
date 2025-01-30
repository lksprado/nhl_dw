WITH source AS (
    SELECT * FROM {{ source('nhl_raw', 'raw_all_game_log') }}
),

renamed AS (

    SELECT
        {{ string_to_int('assists') }} AS assists,
        "commonName.default" AS player_team_common_name,
        to_date(source."gameDate", 'YYYY-MM-DD') AS game_date,
        {{ string_to_int('"gameId"') }} AS game_id,
        {{ string_to_int('"gameTypeId"') }}  AS game_type_id,
        CASE
            WHEN {{ string_to_int('"gameTypeId"') }} = 1 THEN 'pre_season'
            WHEN {{ string_to_int('"gameTypeId"') }} = 2 THEN 'regular_season'
            WHEN {{ string_to_int('"gameTypeId"') }} = 3 THEN 'post_season'
        END AS game_type_name,
        {{ string_to_int('"gameWinningGoals"') }} AS game_winning_goals,
        {{ string_to_int('"goals"') }} AS goals,
        source."homeRoadFlag" AS home_or_road,
        source."opponentAbbrev" AS opponent_team_id,
        "opponentCommonName.default" AS opponent_team_common_name,
        {{ string_to_int('"otGoals"') }} AS ot_goals,
        {{ string_to_int('"pim"') }} AS penalty_minutes,
        {{ string_to_int('"points"') }} AS points,
        {{ string_to_int('"powerPlayPoints"') }} AS powerplay_points,
        {{ string_to_int('"seasonId"') }} AS season_id,
        {{ string_to_int('"shorthandedPoints"') }} AS shorthanded_points,
        source."teamAbbrev" AS player_team_id,
        {{ string_to_int('"plusMinus"') }} AS plus_minus,
        {{ string_to_int('"shots"') }} AS shots,
        {{ string_to_int('"shifts"') }} AS shifts,
        {{ string_to_sec('"toi"') }} AS time_on_ice_sec,
        source.decision,
        {{ string_to_int('"gamesStarted"') }} AS games_started, -- flag
        {{ string_to_int('"goalsAgainst"') }} AS goals_against,
        {{ string_to_float('"savePctg"') }} AS save_pctg,
        {{ string_to_int('"shotsAgainst"') }} AS shots_against,
        {{ string_to_int('"shutouts"') }} AS shutouts,
        replace(source.filename, 'game_log_', '') AS filename
    FROM source
    WHERE source.stats = 'gameLog'
)

  SELECT
    *,
    {{ string_to_int('SUBSTRING("filename" FROM 5 FOR 7)') }} AS player_id,
    concat(game_id::text, substring(filename FROM 5 FOR 7)) AS pk
FROM renamed
