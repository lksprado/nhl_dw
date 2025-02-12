with
team_stats as (
    select * from {{ ref('int_fct_team_stats')}}
),
teams as (
    select * from {{ ref('int_dime_teams')}}
)
final as (
    select
    ts.*,
    t.full_name,
    t.team_id
    from team_stats ts
    left join teams t
    on ts.team_id = t.team_code
)
