with
players as (
    select
    player_id,
    first_name,
    last_name,
    full_name,
    is_active,
    position,
    height_centimeters,
    height_inches,
    weight_kg,
    weight_pounds,
    birth_date,
    birth_country,
    shoots_or_catches,
    latest_season,
    current_team_trid
    from {{ ref('stg_csv__player_info') }}
)
select * from players
