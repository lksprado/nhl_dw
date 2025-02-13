with
goalies as (
    select * from {{ ref('stg_csv__all_goalies_stats') }}
),
fights as (
    SELECT
    concat(((SUBSTRING(game_id::text from 1 for 4)::int)-1)::text,(SUBSTRING(game_id::text from 1 for 4)::int)::text)::int as season_id,
    penalty_by_player_id,
    count(penalty_by_player_id) AS fights
    FROM
    {{ref('stg_csv__play_by_play')}}
    WHERE
    penalty_description = 'fighting'
    GROUP BY
    concat(((SUBSTRING(game_id::text from 1 for 4)::int)-1)::text,(SUBSTRING(game_id::text from 1 for 4)::int)::text)::int,
    penalty_by_player_id
),
boxscore_players as (
    select
    concat(((SUBSTRING(game_id::text from 1 for 4)::int)-1)::text,(SUBSTRING(game_id::text from 1 for 4)::int)::text)::int as season_id,
    player_id,
    sum(goals_against) as goals_against,
    case
        when sum(CAST(SPLIT_PART(even_strength_shots_against, '/', 2) AS NUMERIC)) = 0 then null
        else sum(CAST(SPLIT_PART(even_strength_shots_against, '/', 1) AS NUMERIC)) / sum(CAST(SPLIT_PART(even_strength_shots_against, '/', 2) AS NUMERIC))
    end as even_strength_save_pctg,
    case
        when sum(CAST(SPLIT_PART(powerplay_shots_against, '/', 2) AS NUMERIC)) = 0 then null
        else sum(CAST(SPLIT_PART(powerplay_shots_against, '/', 1) AS NUMERIC)) / sum(CAST(SPLIT_PART(powerplay_shots_against, '/', 2) AS NUMERIC))
    end as powerplay_save_pctg,
    case
        when sum(CAST(SPLIT_PART(shorthanded_shots_against, '/', 2) AS NUMERIC)) = 0 then null
        else sum(CAST(SPLIT_PART(shorthanded_shots_against, '/', 1) AS NUMERIC)) / sum(CAST(SPLIT_PART(shorthanded_shots_against, '/', 2) AS NUMERIC))
    end as shorthanded_save_pctg,
    sum(shots_against) as shots_against
    from {{ref('stg_csv__all_boxscore_players')}}
    group by
    concat(((SUBSTRING(game_id::text from 1 for 4)::int)-1)::text,(SUBSTRING(game_id::text from 1 for 4)::int)::text)::int,
    player_id
)
select
t.*,
t2.fights,
t3.goals_against,
t3.even_strength_save_pctg,
t3.powerplay_save_pctg,
t3.shorthanded_save_pctg,
t3.shots_against
from goalies t
left join fights t2
on t.player_id = t2.penalty_by_player_id and t.season_id = t2.season_id
left join boxscore_players t3
on t.player_id = t3.player_id and t.season_id = t3.season_id
