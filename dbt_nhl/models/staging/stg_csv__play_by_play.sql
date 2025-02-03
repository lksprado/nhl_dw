with source as (
        select * from {{ source('nhl_raw', 'raw_play_by_play') }}
  ),
  renamed as (
      select
      {{string_to_int('SUBSTRING("filename" FROM 5 FOR 10)') }} AS game_id
      ,CASE
            WHEN timeinperiod ~ '^[0-9]{1,2}:[0-9]{2}$'
            AND substring(timeinperiod from 1 for 2) ~ '^[0-9]+$'
            AND substring(timeinperiod from 4 for 2) ~ '^[0-9]+$'
            AND substring(timeinperiod from 1 for 2)::integer < 60
            AND substring(timeinperiod from 4 for 2)::integer < 60
            THEN make_time(0, substring(timeinperiod from 1 for 2)::integer, substring(timeinperiod from 4 for 2)::integer)::time
            ELSE NULL
      END AS time_in_period_time
      ,CASE
            WHEN timeremaining ~ '^[0-9]{1,2}:[0-9]{2}$'
            AND substring(timeremaining from 1 for 2) ~ '^[0-9]+$'
            AND substring(timeremaining from 4 for 2) ~ '^[0-9]+$'
            AND substring(timeremaining from 1 for 2)::integer < 60
            AND substring(timeremaining from 4 for 2)::integer < 60
            THEN make_time(0, substring(timeremaining from 1 for 2)::integer, substring(timeremaining from 4 for 2)::integer)::time
            ELSE NULL
      END AS time_remaining
      ,{{ string_to_int('"sortorder"') }} as sort_order
      ,{{ string_to_int("perioddescriptor_number") }} as period
      ,"perioddescriptor_periodtype" as period_description
      ,{{ string_to_int("typecode") }} as event_code
      ,"typedesckey" as event_description
      ,"details_reason" as event_reason
      ,"details_secondaryreason" as event_secondary_reason
      ,CASE
            WHEN "details_typecode" = 'MAJ' then 'major'
            WHEN "details_typecode" = 'MIN' then 'minor'
            WHEN "details_typecode" = 'MIS' then 'misconduct'
            WHEN "details_typecode" = 'GAM' then 'game misconduct'
            WHEN "details_typecode" = 'PS' then 'penalty shot'
            WHEN "details_typecode" = 'MAT' then 'match'
            WHEN "details_typecode" = 'GRO' then 'gross'
            WHEN "details_typecode" = 'BEN' then 'bench minor'
      END as penalty_type
      ,"details_desckey" as penalty_description
      ,{{ string_to_int("details_duration") }} as penalty_duration
      ,{{ string_to_int("details_committedbyplayerid") }} as penalty_by_player_id
      ,{{ string_to_int("details_drawnbyplayerid") }} as penalty_drawn_by_player_id
      ,{{ string_to_int("details_servedbyplayerid") }} as penalty_served_by_player_id
      ,{{ string_to_int("details_eventownerteamid") }} as event_teamid
      ,{{ string_to_int("details_scoringplayerid") }} as scoring_player_id
      ,{{ string_to_int("details_scoringplayertotal") }} as scoring_player_season_total
      ,{{ string_to_int("details_goalieinnetid") }} as goalie_in_net_player_id
      ,{{ string_to_int("details_awayscore") }} as away_score_at
      ,{{ string_to_int("details_homescore") }} as home_score_at
      ,{{ string_to_int("details_assist1playerid") }} as assist_1_player_id
      ,{{ string_to_int("details_assist1playertotal") }} as assist_1_player_season_total
      ,{{ string_to_int("details_losingplayerid") }} as faceoff_losing_player_id
      ,{{ string_to_int("details_winningplayerid") }} as faceoff_winning_player_id
      ,{{ string_to_int("details_xcoord") }} as event_x_coordinate
      ,{{ string_to_int("details_ycoord") }} as event_y_coordinate
      ,details_zonecode as event_zone_code
      ,{{ string_to_int("details_hittingplayerid") }} as hitting_player_id
      ,{{ string_to_int("details_hitteeplayerid") }} as hittee_player_id
      ,{{ string_to_int("details_blockingplayerid") }} as blocking_player_id
      ,{{ string_to_int("details_shootingplayerid") }} as shooting_player_id
      ,{{ string_to_int("details_playerid") }} as event_player_id
      ,"details_shottype" as shot_type
      ,{{ string_to_int("details_awaysog") }} as away_sog_at
      ,{{ string_to_int("details_homesog") }} as home_sog_at
      ,{{ string_to_int("details_assist2playerid") }} as assist_2_player_id
      ,{{ string_to_int("details_assist2playertotal") }} as assist_2_player_season_total
      ,"hometeamdefendingside" as home_defending_side
      ,"details_highlightclipsharingurl" as details_highlight_clip_sharing_url
      from source
order by game_id,sort_order
  )
  select * from renamed
