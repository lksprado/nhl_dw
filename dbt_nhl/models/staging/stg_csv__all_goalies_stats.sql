with source as (
    select * from {{ source ('nhl_raw', 'raw_all_goalies_stats')}}
),
renamed as (
    select
    concat(playerid,seasonid) as pk,
    cast(playerid AS int) AS player_id,
    cast(seasonid AS int) AS season_id,
    goaliefullname AS full_name,
    shootscatches AS shoots_or_catches,
    'G' as position_code,
    {{ string_to_int('gamesplayed') }}  AS games_played,
    {{ string_to_int('gamesstarted') }}  AS games_started,
    {{ string_to_int('wins') }}  AS wins,
    {{ string_to_int('losses') }}  AS losses,
    {{ string_to_int('otlosses') }}  AS ot_losses,
    {{ string_to_int('ties') }}  AS ties,
    {{ string_to_int('saves') }}  AS saves,
    {{ string_to_int('shotsagainst') }} as shots_against,
    --{{ string_to_float('savepct') }}  AS save_pctg,
    {{ string_to_int('goalsagainst') }}  AS goals_against,
    --{{ string_to_float('goalsagainstaverage') }}  AS goals_against_average,
    {{ string_to_int('shutouts') }}  AS shutouts,
    {{ string_to_int('timeonice') }}/60  AS time_on_ice_minutes,
    {{ string_to_int('penaltyminutes') }}  AS penalty_minutes,
    {{ string_to_int('points') }}  AS points,
    cast(assists AS int) AS assists,
    {{ string_to_int('goals') }}  AS goals
    from source
),
grouping as (
    select
    pk,
    player_id,
    season_id,
    full_name,
    shoots_or_catches,
    position_code,
    sum(games_played) as games_played,
    sum(games_started) as games_started,
    sum(wins) as wins,
    sum(losses) as losses,
    sum(ot_losses) as ot_losses,
    sum(ties) as ties,
    sum(saves) as saves,
    CASE
        WHEN sum(shots_against) = 0 THEN NULL
        ELSE  round(sum(saves) / sum(shots_against)::NUMERIC(10,3),3)
    END as save_pctg,
    sum(goals_against),
    CASE
        WHEN sum(time_on_ice_minutes) = 0 THEN NULL
        ELSE round((sum(goals_against) * 60)::NUMERIC(10,3) / sum(time_on_ice_minutes),3)
    END  as goals_against_average,
    sum(shutouts) as shutouts,
    sum(time_on_ice_minutes) as time_on_ice_minutes,
    sum(penalty_minutes) as penalty_minutes,
    sum(points) as points,
    sum(assists) as assists,
    sum(goals) as goals
    from renamed
    group by
    pk,
    season_id,
    player_id,
    full_name,
    shoots_or_catches,
    position_code
)
SELECT * FROM grouping
