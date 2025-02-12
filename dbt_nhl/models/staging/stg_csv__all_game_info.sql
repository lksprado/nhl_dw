WITH source AS (
    SELECT * FROM {{ source('nhl_raw', 'raw_game_info') }}
),

renamed AS (
    SELECT
        {{ string_to_int('id') }} AS game_id,
        {{ string_to_int('season') }} AS season_id,
        to_date(gamedate, 'YYYY-MM-DD') AS game_date,
        (
            to_timestamp(easternstarttime, 'YYYY-MM-DDTHH24:MI:SS') + interval '0 hours'
        )::timestamp AS start_time_date_eastern,
        (
            to_timestamp(easternstarttime, 'YYYY-MM-DDTHH24:MI:SS') + interval '2 hours'
        )::timestamp AS start_time_date_gmt_minus_3,
        CASE
            WHEN {{ string_to_int('gametype') }} = 1 THEN 'pre'
            WHEN {{ string_to_int('gametype') }} = 2 THEN 'regular'
            WHEN {{ string_to_int('gametype') }} = 3 THEN 'playoffs'
        END as game_type,
        {{ string_to_int('gamenumber') }} AS game_number,
        {{ string_to_int('gameschedulestateid') }} AS game_schedule_state_id,
        {{ string_to_int('gamestateid') }} AS game_state_id,
        {{ string_to_int('period') }} AS number_of_periods,
        {{ string_to_int('hometeamid') }} AS home_team_id,
        {{ string_to_int('visitingteamid') }} AS away_team_id,
        {{ string_to_int('homescore') }} AS home_score,
        {{ string_to_int('visitingscore') }} AS away_score
    FROM source
    WHERE gamestateid IN ('1','7')
    AND gameschedulestateid = '1'
    AND gametype IN ('2','3')
)
SELECT * FROM renamed where game_date < current_date
