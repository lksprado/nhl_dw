with
skaters as (
    select * from {{ ref('stg_csv__all_skaters_stats') }}
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
    sum(hits) as hits,
    sum(sog) as sog,
    sum(blocked_shots) as blocked_shots,
    sum(shifts) as shifts,
    sum(giveaways) as giveaways,
    sum(takeaways) as takeaways
    from {{ref('stg_csv__all_boxscore_players')}}
    group by
    concat(((SUBSTRING(game_id::text from 1 for 4)::int)-1)::text,(SUBSTRING(game_id::text from 1 for 4)::int)::text)::int,
    player_id
)
select
t.*,
t2.fights,
t3.hits,
t3.sog,
t3.blocked_shots,
t3.shifts,
t3.giveaways,
t3.takeaways
from skaters t
left join fights t2
on t.player_id = t2.penalty_by_player_id and t.season_id = t2.season_id
left join boxscore_players t3
on t.player_id = t3.player_id and t.season_id = t3.season_id
