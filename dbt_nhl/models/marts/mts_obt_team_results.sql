with
games as (
    select * from {{ ref('int_fct_games') }}
),
teams as (
    select * from {{ ref('int_dime_teams') }}
),
union_tab as (
    SELECT
    season_id,
    home_team_id AS team_id,
    game_type,
    'home' AS home_or_away,
    count(game_id) AS games_played,
    sum(home_score) AS goals,
    sum(home_sog) AS sog,
    sum(home_hits) AS hits,
    round(avg(home_faceoff_winning_pctg),2) AS faceoff_winning_pctg,
    round(avg(home_powerplay_pctg),2) AS powerplay_pctg,
    sum(home_penalty_minutes) AS penalty_minutes,
    sum(home_blocked_shots) AS blocked_shots,
    sum(home_giveaways) AS giveaways,
    sum(home_takeaways) AS takeaways
    FROM games
    GROUP BY
    season_id,
    home_team_id,
    game_type
    UNION ALL
    SELECT
    season_id,
    away_team_id AS team_id,
    game_type,
    'away' AS home_or_away,
    count(game_id) AS games_played,
    sum(away_score) AS goals,
    sum(away_sog) AS sog,
    sum(away_hits) AS hits,
    round(avg(away_faceoff_winning_pctg),2) AS faceoff_winning_pctg,
    round(avg(away_powerplay_pctg),2) AS powerplay_pctg,
    sum(away_penalty_minutes) as penalty_minutes,
    sum(away_blocked_shots) as blocked_shots,
    sum(away_giveaways) as giveaways,
    sum(away_takeaways) as takeaways
    FROM games
    GROUP BY
    season_id,
    away_team_id,
    game_type
)
select
season_id,
t1.team_id,
t2.team_trid as team_trid,
t2.short_name as team_shortname,
t2.full_name as team_fullname,
game_type,
home_or_away,
games_played,
goals,
sog,
hits,
faceoff_winning_pctg,
powerplay_pctg,
penalty_minutes,
blocked_shots,
giveaways,
takeaways
from union_tab t1
left join teams t2
on t1.team_id = t2.team_id
