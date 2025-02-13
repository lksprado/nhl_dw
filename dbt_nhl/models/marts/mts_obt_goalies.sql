with
goalies as (
    select * from {{ ref('int_fct_goalies')}}
),
player as (
    select * from {{ref('int_dime_players')}}
),
teams as (
    select * from {{ref('int_dime_teams')}}
)
select
t.*,
t2.is_active,
t2.position,
t2.height_centimeters,
t2.height_inches,
t2.weight_kg,
t2.weight_pounds,
t2.birth_date,
t2.birth_country,
t2.latest_season
from goalies t
left join player t2
on t.player_id = t2.player_id
