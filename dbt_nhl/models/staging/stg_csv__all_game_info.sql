WITH source AS (
    SELECT * FROM {{ source('nhl_raw', 'raw_game_info') }}
),

renamed AS (
    SELECT
        {{ string_to_int('"id"') }} AS game_id,
        {{ string_to_int('"season"') }} AS season_id,
        to_date("gameDate", 'YYYY-MM-DD') AS game_date,
        (
            to_timestamp("easternStartTime", 'YYYY-MM-DD"T"HH24:MI:SS') + interval '0 hours'
        )::timestamp AS start_time_date_eastern,
        (
            to_timestamp("easternStartTime", 'YYYY-MM-DD"T"HH24:MI:SS') + interval '2 hours'
        )::timestamp AS start_time_date_gmt_minus_3,
        CASE
            WHEN {{ string_to_int('"gameType"') }} = 1 THEN 'pre_season'
            WHEN {{ string_to_int('"gameType"') }} = 2 THEN 'regular_season'
            WHEN {{ string_to_int('"gameType"') }} = 3 THEN 'post_season'
        END as game_type_name,
        {{ string_to_int('"gameType"') }} as game_type,
        {{ string_to_int('"gameNumber"') }} AS game_number,
        {{ string_to_int('"gameScheduleStateId"') }} AS game_schedule_state_id,
        {{ string_to_int('"gameStateId"') }} AS game_state_id,
        {{ string_to_int('"period"') }} AS number_of_periods,
        {{ string_to_int('"homeTeamId"') }} AS home_team_id,
        {{ string_to_int('"visitingTeamId"') }} AS away_team_id,
        {{ string_to_int('"homeScore"') }} AS home_score,
        {{ string_to_int('"visitingScore"') }} AS away_score
    FROM source
    WHERE "gameStateId" IN ('1','7')
    AND "gameScheduleStateId" = '1'
    AND "gameType" IN ('2','3')
)
SELECT * FROM renamed where game_date < current_date
