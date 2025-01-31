with
game_info as (
    select * from {{ ref('stg_csv__all_game_info')}}
),
game_sogs as (
    select * from {{ ref('stg_csv__game_info_sog')}}
),
game_details as (
    select * from {{ ref('stg_csv__game_details') }}
),
final as (
    select
    gi.game_id,
    gi.season_id,
    gi.game_date,
    gi.start_time_date_eastern,
    gi.start_time_date_gmt_minus_3,
    gi.game_type,
    gi.game_number,
    gi.number_of_periods,
    gi.home_team_id,
    gi.away_team_id,
    gi.away_score,
    gi.home_score,
    case when gs.home_sog is null then gd.home_sog else gs.home_sog end as home_sog,
    case when gs.away_sog is null then gd.away_sog else gs.away_sog end as away_sog,
    gd.referee_1,
    gd.referee_2,
    gd.referee_3,
    gd.home_score_period_1,
    gd.home_score_period_2,
    gd.home_score_period_3,
    gd.home_score_period_4,
    gd.home_score_period_5,
    gd.away_score_period_1,
    gd.away_score_period_2,
    gd.away_score_period_3,
    gd.away_score_period_4,
    gd.away_score_period_5,
    gd.home_shots_period_1,
    gd.home_shots_period_2,
    gd.home_shots_period_3,
    gd.home_shots_period_4,
    gd.home_shots_period_5,
    gd.away_shots_period_1,
    gd.away_shots_period_2,
    gd.away_shots_period_3,
    gd.away_shots_period_4,
    gd.away_shots_period_5,
    gd.home_powerplay,
    gd.away_powerplay,
    gd.home_powerplay_pctg,
    gd.away_powerplay_pctg,
    gd.home_faceoff_winning_pctg,
    gd.away_faceoff_winning_pctg,
    gd.home_hits,
    gd.away_hits,
    gd.home_giveaways,
    gd.away_giveaways,
    gd.home_takeaways,
    gd.away_takeaways,
    gd.away_penalty_minutes,
    gd.home_penalty_minutes,
    gd.home_blocked_shots,
    gd.away_blocked_shots
    from game_info gi
    left join game_sogs gs
    on gi.game_id = gs.game_id
    left join game_details gd
    on gi.game_id = gd.game_id
    where game_date < now()
    order by game_id desc
)
select
*,
{{ dbt_utils.generate_surrogate_key(['home_team_id','season_id']) }} as sk_home_teamid_seasonid,
{{ dbt_utils.generate_surrogate_key(['away_team_id','season_id']) }} as sk_away_teamid_seasonid
from final
