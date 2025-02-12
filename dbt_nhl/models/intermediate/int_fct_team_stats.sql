with
club_stats as (
    select
    season_id,
    max(games_played) as games_played_game_type,
    game_type,
    team_trid,
    sum(goals) as goals,
    sum(assists) as assists,
    sum(points) as player_points,
    sum(plus_minus) as plus_minus,
    sum(penalty_minutes) as penalty_minutes,
    sum(powerplay_goals) as powerplays_goas,
    sum(ot_goals) as ot_goals,
    sum(shots) as shots,
    case
      when sum(shots) = 0 then null
      else round(sum(goals)::numeric / sum(shots)::numeric,3)
    end as shooting_pctg,
    round(avg(faceoff_win_pctg)::numeric,3) as faceoff_win_pctg,
    sum(goals_against) as goals_against,
    sum(saves) as saves,
    case
      when sum(shots) = 0 then null
      else round(sum(saves)::numeric / sum(shots)::numeric,3)
    end as save_pctg,
    sum(shots_against) as shots_against
    from {{ ref('stg_csv__all_club_stats') }}
    group by
    season_id,
    game_type,
    team_trid
)
select * from club_stats
