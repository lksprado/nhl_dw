with source as (
        select * from {{ source('nhl_raw', 'raw_all_game_log') }}
),
renamed as (

    select
        {{ string_to_int('assists') }} assists,
        "commonName.default" AS player_team_common_name,
        to_date("gameDate",'YYYY-MM-DD') AS game_date,
        {{ string_to_int('"gameId"') }} as game_id,
        {{ string_to_int('"gameTypeId"') }}  AS game_type_id,
        CASE
            WHEN {{ string_to_int('"gameTypeId"') }} = 1 THEN 'pre_season'
            WHEN {{ string_to_int('"gameTypeId"') }} = 2 THEN 'regular_season'
            WHEN {{ string_to_int('"gameTypeId"') }} = 3 THEN 'post_season'
        END AS game_type_name,
        {{ string_to_int('"gameWinningGoals"') }} as game_winning_goals,
        {{ string_to_int('"goals"') }} as goals,
        "homeRoadFlag" AS home_or_road,
        "opponentAbbrev" AS "opponent_team_id",
        "opponentCommonName.default" AS "opponent_team_common_name",
        {{ string_to_int('"otGoals"') }} as ot_goals,
        {{ string_to_int('"pim"') }} as penalty_minutes,
        {{ string_to_int('"points"') }} as points,
        {{ string_to_int('"powerPlayPoints"') }} as powerplay_points,
        {{ string_to_int('"seasonId"') }} as season_id,
        {{ string_to_int('"shorthandedPoints"') }} as shorthanded_points,
        "teamAbbrev" AS "player_team_id",
        {{ string_to_int('"plusMinus"') }} as plus_minus,
        {{ string_to_int('"shots"') }} as shots,
        {{ string_to_int('"shifts"') }} as shifts,
        {{ string_to_sec('"toi"') }} as time_on_ice_sec,
        "decision",
        {{ string_to_int('"gamesStarted"') }} as games_started, -- flag
        {{ string_to_int('"goalsAgainst"') }} as goals_against,
        {{ string_to_float('"savePctg"') }} as save_pctg,
        {{ string_to_int('"shotsAgainst"') }} as shots_against,
        {{ string_to_int('"shutouts"') }} as shutouts,
        REPLACE("filename",'game_log_','') as filename
    from source
    where "stats" = 'gameLog'
)
  select *,
    {{string_to_int('SUBSTRING("filename" FROM 5 FOR 7)') }} AS player_id,
    concat(game_id::text ,SUBSTRING("filename" FROM 5 FOR 7)) as pk from renamed
