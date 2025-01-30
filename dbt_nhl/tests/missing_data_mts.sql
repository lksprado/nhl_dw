SELECT
home_sog
from {{ ref('mts.obt_game_results') }}
where extract(year from  game_date) >= 2024
and home_sog is null
