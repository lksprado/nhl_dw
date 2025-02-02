with
team_stats as (
    select * from {{ ref('stg_csv__all_team_stats') }}
)
select * from team_stats
