with
source as (
    select * from {{ source('nhl_raw','raw_boxscore_games') }}
),
renamed as (
    select
    {{ string_to_int ('id') }} as game_id,
    {{ string_to_int ('season') }} as season_id,
    case
        when {{ string_to_int ('gametype') }} = 2 then 'regular season'
        when {{ string_to_int ('gametype') }} = 3 then 'post season'
    end as game_type,
    to_date(gamedate,'YYYY-MM-DD') as game_date,
    (to_timestamp(starttimeutc, 'YYYY-MM-DD"T"HH24:MI:SS'))::timestamp AS start_time_utc,
    (to_timestamp(starttimeutc, 'YYYY-MM-DD"T"HH24:MI:SS') - interval '3 hours')::timestamp AS start_time_date_gmt_minus_3,
    (to_timestamp(starttimeutc, 'YYYY-MM-DD"T"HH24:MI:SS') - interval '5 hours')::timestamp AS start_time_date_eastern,
    venue_default as arena,
    venuelocation_default as city,
    gameoutcome_lastperiodtype as outcome_last_period_type,
    {{ string_to_int ('gameoutcome_otperiods') }} as outcome_ot_periods,
    {{ string_to_int ('awayteam_id') }} as away_team_code,
    {{ string_to_int ('hometeam_id') }} as home_team_code,
    {{ string_to_int ('awayteam_score') }} as away_score,
    {{ string_to_int ('hometeam_score') }} as home_score,
    {{ string_to_int ('awayteam_sog') }} as away_sog,
    {{ string_to_int ('hometeam_sog') }} as home_sog,
    filename
    from source
)
select * from renamed
