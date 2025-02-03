with source as (
        select * from {{ source('nhl_raw', 'raw_all_club_stats') }}
  ),
  renamed as (
      select
    concat("playerId","season") as pk,
    cast("playerId" AS int) AS player_id,
    CAST("season" AS int) AS season_id,
    "firstName.default" as first_name,
    "lastName.default" as last_name,
    CASE
        WHEN {{ string_to_int('"gameType"') }} = 1 THEN 'pre_season'
        WHEN {{ string_to_int('"gameType"') }} = 2 THEN 'regular_season'
        WHEN {{ string_to_int('"gameType"') }} = 3 THEN 'post_season'
    END as game_type,
    {{ string_to_int('"gameWinningGoals"') }} as game_winning_goals,
    {{ string_to_int('"gamesPlayed"') }} as games_played ,
    {{ string_to_int('"gamesStarted"') }} as games_started,
    {{ string_to_int('"goals"') }} as goals,
    {{ string_to_int('"goalsAgainst"') }} as goals_against,
    {{ string_to_float('"goalsAgainstAverage"') }} as goals_against_average,
    "headshot" as pic_url,
    {{ string_to_int('"overtimeGoals"') }} as ot_goals,
    {{ string_to_int('"penaltyMinutes"') }} as penalty_minutes,
    {{ string_to_int('"plusMinus"') }} as plus_minus,
    {{ string_to_int('"points"') }} as points,
    "positionCode" as position_code,
    {{ string_to_int('"powerPlayGoals"') }} as  powerplay_goals,
    {{ string_to_float('"savePercentage"') }} as save_pctg,
    {{ string_to_int('"saves"') }} as saves,
    {{ string_to_float('"shootingPctg"') }} as shooting_pctg,
    {{ string_to_int('"shorthandedGoals"') }} as shorthanded_goals,
    {{ string_to_int('"shots"') }} as shots,
    {{ string_to_int('"shotsAgainst"') }} as shots_against,
    {{ string_to_int('"shutouts"') }} as shutouts,
    "stats" as role,
    {{ string_to_int('"ties"') }} as ties,
    {{ string_to_int('"timeOnIce"') }} as time_on_ice,
    {{ string_to_int('"wins"') }} as wins
    from source
  )
  select * from renamed
