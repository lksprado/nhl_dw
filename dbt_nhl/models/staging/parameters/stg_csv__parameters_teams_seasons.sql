with source as (
        select * from {{ source('nhl_raw', 'parameters_team_season') }}
  ),
  max_season as (
      select max(season_id) as max_season_id from source
  ),
  renamed as (
      select
          team_id,
          min(season_id) as first_season,
          max(season_id) as latest_season,
          case
              when max(season_id) = (select max_season_id from max_season) then true
              else false
          end as is_active
      from source
      group by team_id
  )
select * from renamed
