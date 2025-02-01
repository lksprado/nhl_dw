with
source as (
    select * from {{ source('nhl_raw','raw_boxscore_players') }}
),
renamed as (
    select
    {{ string_to_int('SUBSTRING(filename FROM 5 FOR 10)') }} as game_id,
    {{ string_to_int ('playerid') }} as player_id,
    {{ string_to_int ('sweaternumber') }} as jersey_number,
    position,
    {{ string_to_int('goals') }} as goals,
    {{ string_to_int('assists') }} as assists,
    {{ string_to_int('points') }} as points,
    {{ string_to_int('plusminus') }} as plus_minus,
    {{ string_to_int('pim') }} as penalty_minutes,
    {{ string_to_int('hits') }} as hits,
    {{ string_to_int('powerplaygoals') }} as powerplay_goals,
    {{ string_to_int('sog') }} as sog,
    {{ string_to_float('faceoffwinningpctg') }} as faceoff_winning_pctg,
    {{ string_to_int('blockedshots') }} as blocked_shots,
    {{ string_to_int('shifts') }} as shifts,
    {{ string_to_int('giveaways') }} as giveaways,
    {{ string_to_int('takeaways') }} as takeways,
    {{ string_to_int('evenstrengthgoalsagainst') }} as even_strenght_goals_against,
    {{ string_to_int('powerplaygoalsagainst') }} as powerplay_goals_against,
    {{ string_to_int('shorthandedgoalsagainst') }} as shorthanded_goals_against,
    {{ string_to_int('goalsagainst') }} as goals_against,
    {{ string_to_min('toi') }} as time_on_ice_minutes,
    starter::boolean,
    decision,
    {{ string_to_int('shotsagainst') }} as shots_against ,
    {{ string_to_int('saves') }} as saves,
    {{ string_to_float('savepctg') }} as save_pctg,
    filename
    from source
)
select * from renamed
