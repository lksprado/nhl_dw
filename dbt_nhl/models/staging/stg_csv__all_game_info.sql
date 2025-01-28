with source as (
        select * from {{ source('nhl_raw', 'raw_game_info') }}
  ),
  renamed as (
      select
        {{ string_to_int('"id"') }} as game_id,
        {{ string_to_int('"season"') }} as season_id,
        to_date("gameDate", 'YYYY-MM-DD') as game_date,
        (to_timestamp("easternStartTime", 'YYYY-MM-DD"T"HH24:MI:SS') + interval '0 hours')::timestamp as start_time_date_eastern,
        (to_timestamp("easternStartTime", 'YYYY-MM-DD"T"HH24:MI:SS') + interval '2 hours')::timestamp as start_time_date_gmt_minus_3,
        {{ string_to_int('"gameType"') }} as game_type,
        {{ string_to_int('"gameNumber"') }} as game_number ,
        {{ string_to_int('"gameScheduleStateId"') }} as game_schedule_state_id ,
        {{ string_to_int('"gameStateId"') }} as game_state_id,
        {{ string_to_int('"period"') }} as number_of_periods,
        {{ string_to_int('"homeTeamId"') }} as home_team_id,
        {{ string_to_int('"visitingTeamId"') }} as visiting_team_id,
        {{ string_to_int('"homeScore"') }} as home_score,
        {{ string_to_int('"visitingScore"') }} as visiting_score
      from source
  )
  select * from renamed
