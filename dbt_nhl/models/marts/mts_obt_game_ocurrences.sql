with
plays as (
    select
    concat(((SUBSTRING(game_id::text from 1 for 4)::int)-1)::text,(SUBSTRING(game_id::text from 1 for 4)::int)::text) as season_id,
    case
        when SUBSTRING(game_id::text from 5 for 2) = '02' then 'regular'
        when SUBSTRING(game_id::text from 5 for 2) = '03' then 'playoffs'
    end as game_type,
    event_description,
    event_team_id,
    count(game_id) as qt_events
    from {{ ref('int_fct_play_by_play') }}
    group by
    concat(((SUBSTRING(game_id::text from 1 for 4)::int)-1)::text,(SUBSTRING(game_id::text from 1 for 4)::int)::text),
    case
        when SUBSTRING(game_id::text from 5 for 2) = '02' then 'regular'
        when SUBSTRING(game_id::text from 5 for 2) = '03' then 'playoffs'
    end,
    event_description,
    event_team_id
),
teams as (
    select * from {{ ref ('int_dime_teams') }}
)
select
season_id,
game_type,
t2.team_trid,
t2.full_name,
t2.short_name,
event_description,
qt_events
from plays t
left join teams t2
on t.event_team_id = t2.team_id
