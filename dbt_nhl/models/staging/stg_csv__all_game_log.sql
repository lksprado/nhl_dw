WITH source AS (
    SELECT * FROM {{ source('nhl_raw', 'raw_game_log') }}
),

renamed AS (

    SELECT
        to_date(gamedate, 'YYYY-MM-DD') AS game_date,
        {{ string_to_int('seasonid') }} AS season_id,
        {{ string_to_int('gameid') }} AS game_id,
        {{ string_to_int('SUBSTRING(filename FROM 5 FOR 7)') }} AS player_id,
        teamabbrev AS player_team_trid,
        commonname_default AS player_team_short_name,
        opponentabbrev AS opponent_team_trid,
        opponentcommonname_default AS opponent_team_short_name,
        CASE
            WHEN {{ string_to_int('gametypeid') }} = 1 THEN 'pre'
            WHEN {{ string_to_int('gametypeid') }} = 2 THEN 'regular'
            WHEN {{ string_to_int('gametypeid') }} = 3 THEN 'playoffs'
        END AS game_type,
        homeroadflag AS home_or_road,
        {{ string_to_int('goals') }} AS goals,
        {{ string_to_int('assists') }} AS assists,
        {{ string_to_int('points') }} AS points,
        {{ string_to_int('plusminus') }} AS plus_minus,
        {{ string_to_int('pim') }} AS penalty_minutes,
        {{ string_to_int('powerplaygoals') }} AS powerplay_goals,
        {{ string_to_int('powerPlayPoints') }} AS powerplay_points,
        {{ string_to_int('shots') }} AS shots,
        {{ string_to_int('shifts') }} AS shifts,
        {{ string_to_int('gamewinninggoals') }} AS game_winning_goals,
        {{ string_to_int('otgoals') }} AS ot_goals,
        {{ string_to_int('shorthandedgoals') }} AS shorthanded_goals,
        {{ string_to_int('shorthandedpoints') }} AS shorthanded_points,
        {{ string_to_int('goalsagainst') }} AS goals_against,
        {{ string_to_sec('toi') }} AS time_on_ice_sec,
        {{ string_to_int('gamesstarted') }} AS starter, -- flag
        decision,
        {{ string_to_int('shotsagainst') }} AS shots_against,
        {{ string_to_float('savepctg') }} AS save_pctg,
        {{ string_to_int('shutouts') }} AS shutouts,
        filename
    FROM source
)
  SELECT
    *
FROM renamed
