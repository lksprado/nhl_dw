with source as (
    select * from {{ source ('nhl_raw', 'raw_all_goalies_stats')}}
),
renamed as (
    select
    concat("playerId","seasonId") as pk,
    cast("playerId" AS int) AS player_id,
    cast("seasonId" AS int) AS season_id,
    "goalieFullName" AS full_name,
    "lastName" AS last_name,
    "shootsCatches" AS shoots_or_catches,
    'G' as position_code,
    {{ string_to_int('"gamesPlayed"') }}  AS games_played,
    {{ string_to_int('"gamesStarted"') }}  AS games_started,
    {{ string_to_int('"wins"') }}  AS wins,
    {{ string_to_int('"losses"') }}  AS losses,
    {{ string_to_int('"otLosses"') }}  AS ot_losses,
    {{ string_to_int('"ties"') }}  AS ties,
    {{ string_to_int('"saves"') }}  AS saves,
    {{ string_to_float('"savePct"') }}  AS save_pctg,
    {{ string_to_int('"goalsAgainst"') }}  AS goals_against,
    {{ string_to_float('"goalsAgainstAverage"') }}  AS goals_against_average,
    {{ string_to_int('"shutouts"') }}  AS shutouts,
    {{ string_to_int('"timeOnIce"') }}  AS time_on_ice,
    {{ string_to_int('"penaltyMinutes"') }}  AS penalty_minutes,
    {{ string_to_int('"points"') }}  AS points,
    cast("assists" AS int) AS assists,
    {{ string_to_int('"goals"') }}  AS goals
    from source
),
dup as (
    {{ dbt_utils.deduplicate(
        relation='renamed',
        partition_by='pk',
        order_by= 'season_id'
        )
    }}
)

SELECT * FROM DUP
