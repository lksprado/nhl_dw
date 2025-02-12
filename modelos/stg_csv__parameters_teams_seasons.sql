WITH
seasons AS (
	SELECT
	season_id,
	game_type,
	home_team_id
	FROM {{ ref('stg_csv__all_game_info') }}
	GROUP BY
	season_id,
	game_type,
	home_team_id
	),
teams AS (
	SELECT
	team_id,
	team_code
	FROM {{ ref('stg_csv__teams') }}
	),
season_teams as (
	SELECT
	s.season_id,
	t.team_id,
	s.game_type
	FROM seasons s
	LEFT JOIN teams t
	ON s.home_team_id = t.team_code
	ORDER BY t.team_id, s.season_id
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
      from season_teams
      group by team_id
  )
select * from renamed
