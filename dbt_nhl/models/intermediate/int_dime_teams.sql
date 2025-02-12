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
	team_trid,
	team_id,
	full_name
	FROM {{ ref('stg_csv__teams') }}
	),
season_teams as (
	SELECT
	s.season_id,
	t.team_id,
	s.game_type
	FROM seasons s
	LEFT JOIN teams t
	ON s.home_team_id = t.team_id
	ORDER BY t.team_id, s.season_id
	),
max_season as (
	select max(season_id) as max_season_id from season_teams
),
first_and_latest_seasons_per_team as (
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
),
short_name as (
	select
	player_team_trid,
	player_team_short_name
	from {{ref('stg_csv__all_game_log')}}
	group by
	player_team_trid,
	player_team_short_name
),
final as (
    select
    t1.team_id,
    t1.team_trid,
    t1.full_name,
	t4.player_team_short_name as short_name,
    t3.first_season,
    t3.latest_season,
    right(t3.latest_season::text,4)::int - left(t3.first_season::text,4)::int as age,
    t3.is_active
    from teams t1
    left join season_teams t2 on t1.team_id = t2.team_id
	left join first_and_latest_seasons_per_team  t3 on t1.team_id = t3.team_id
	left join short_name t4 on t1.team_trid = t4.player_team_trid
	where t3.is_active is not null
	group by
    t1.team_id,
    t1.team_trid,
    t1.full_name,
    t3.first_season,
    t3.latest_season,
    right(t3.latest_season::text,4)::int - left(t3.first_season::text,4)::int,
    t3.is_active,
	t4.player_team_short_name
)
select * from final
