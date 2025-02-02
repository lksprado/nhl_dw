with
game_boxscore as (
    select * from {{ ref('stg_csv__all_boxscore_games')}}
),
game_details as (
    select * from {{ ref('stg_csv__game_details') }}
),
final as (
    select
    gb.game_id,--
    gb.season_id,--
    gb.game_date,--
    gb.start_time_date_eastern,
    gb.start_time_date_gmt_minus_3,
    gb.arena,
    gb.city,
    gb.game_type, --
    gb.outcome_last_period_type,
    gb.outcome_ot_periods,
    gb.home_team_code,
    gb.away_team_code,
    gb.away_score,
    gb.home_score,
    gb.away_sog,
    gb.home_sog,
    gd.referee_1,
    gd.referee_2,
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
    from game_boxscore gb
    left join game_details gd
    on gb.game_id = gd.game_id
)
select
*
from final
