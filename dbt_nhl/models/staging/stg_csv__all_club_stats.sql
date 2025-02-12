with source as (
        select * from {{ source('nhl_raw', 'raw_all_club_stats') }}
  ),
  renamed as (
      select
    concat("playerid","season","gametype") as sk_playerid_seasonid_gametypeid,
    cast("playerid" as int) as player_id,
    cast("season" as int) as season_id,
    SUBSTRING(filename from 16 for 3) as team_trid,
    case
      when positioncode is null and gamesstarted is not null or saves is not null then 'G'
      else positioncode
    end  as position_code,
    {{ string_to_int('gamesplayed') }} as games_played,
    case
        when {{ string_to_int('gametype') }} = 1 then 'pre'
        when {{ string_to_int('gametype') }} = 2 then 'regular'
        when {{ string_to_int('gametype') }} = 3 then 'playoffs'
    end as game_type,
    {{ string_to_int('gametype') }} as game_type_id,
    {{ string_to_int('goals') }} as goals,
    {{ string_to_int('assists') }} as assists,
    {{ string_to_int('points') }} as points,
    {{ string_to_int('plusminus') }} as plus_minus,
    {{ string_to_int('penaltyminutes') }} as penalty_minutes,
    {{ string_to_int('powerplaygoals') }} as  powerplay_goals,
    {{ string_to_int('shorthandedgoals') }} as shorthanded_goals,
    {{ string_to_int('gamewinninggoals') }} as game_winning_goals,
    {{ string_to_int('overtimegoals') }} as ot_goals,
    {{ string_to_int('shots') }} as shots,
    case
      when {{ string_to_float('shots') }} = 0 then null
      else round({{ string_to_int('goals') }}::numeric / {{ string_to_int('shots') }}::numeric,3)
    end as shooting_pctg,
    {{ string_to_float('faceoffwinpctg') }} as faceoff_win_pctg,
    {{ string_to_int('gamesstarted') }} as games_started,
    {{ string_to_int('wins') }} as wins,
    {{ string_to_int('losses') }} as losses,
    {{ string_to_int('overtimelosses') }} as ot_losses,
    {{ string_to_int('ties') }} as ties,
    {{ string_to_int('goalsagainst') }} as goals_against,
    {{ string_to_float('goalsagainstaverage') }} as goals_against_average,
    {{ string_to_int('saves') }} as saves,
    {{ string_to_float('savepercentage') }} as save_pctg,
    {{ string_to_int('shotsagainst') }} as shots_against,
    {{ string_to_int('shutouts') }} as shutouts,
    case
      when timeonice is null then ({{ string_to_float('avgtimeonicepergame') }} * {{ string_to_int('gamesplayed') }})/60::int
    else {{ string_to_int('timeonice') }}/60
    end as time_on_ice_minutes,
    "headshot" as pic_url
    from source
  )
  select * from renamed
