with
teams as (
    select * from {{ref('stg_csv__teams')}}
),
team_season as (
    select * from {{ ref('stg_csv__parameters_teams_seasons')}}
),
gamelog_teams as (
    select
    player_team_id,
    player_team_common_name
    from {{ ref('stg_csv__all_game_log')}}
    group by
    player_team_id,
    player_team_common_name
),
final as (
    select
    t1.team_id,
    t1.team_code,
    t1.full_name,
    t3.player_team_common_name as common_name,
    t2.first_season,
    t2.latest_season,
    right(t2.latest_season::text,4)::int - left(t2.first_season::text,4)::int as age,
    t2.is_active
    from teams t1
    left join team_season t2 on t1.team_id = t2.team_id
    left join gamelog_teams t3 on t1.team_id = t3.player_team_id
    where is_active is not null
)
select * from final
