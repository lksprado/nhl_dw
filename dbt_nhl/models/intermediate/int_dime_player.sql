with
player_info as (
    select
    player_id,
    first_name,
    last_name,
    concat(first_name,' ',last_name) as full_name,
    pi.is_active,
    extract(year from age(now(), birth_date)) as age,
    pi.latest_season,
    current_team_id,
    t.full_name as current_team_name,
    birth_date,
    birth_country,
    birth_state,
    birth_city,
    position,
    height_inches,
    weight_pounds,
    height_centimeters,
    weight_kg,
    shoots_or_catches,
    in_top100_alltime,
    in_hall_of_fame,
    draft_overall_pick,
    draft_pickinround,
    draft_round,
    draft_team_id,
    t2.full_name as draft_team_name,
    pic_url
    from {{ ref('stg_csv__player_info') }} pi
    left join {{ ref('int_dime_teams') }} t
    on pi.current_team_id = t.team_id
    left join {{ ref('int_dime_teams') }} t2
    on pi.draft_team_id = t2.team_id
),
player_seasons as (
    select
    player_id,
    season_id,
    rank() over (partition by player_id order by season_id) as seasons_played,
    position_code
    from {{ ref('stg_csv__all_skaters_stats')}}
    union all
    select
    player_id,
    season_id,
    rank() over (partition by player_id order by season_id) as seasons_played,
    position_code
    from {{ ref('stg_csv__all_goalies_stats')}}
)
select
pi.*,
ps.seasons_played
from player_info pi
left join player_seasons ps
on pi.player_id = ps.player_id
and pi.latest_season = ps.season_id
and pi.position = ps.position_code
