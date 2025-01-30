with source as (
        select * from {{ source('nhl_raw', 'raw_play_by_play') }}
  ),
  renamed as (
      select
        {{string_to_int('SUBSTRING("filename" FROM 5 FOR 10)') }} AS game_id,
        {{ string_to_int("awayteam_id") }} as away_team_code,
        "awayteam_abbrev"::text as away_team_id,
        concat("awayteam_placename_default",' ',"awayteam_commonname_default") as away_team_full_name,
        {{ string_to_int("hometeam_id") }} as home_team_code,
        "hometeam_abbrev"::text as home_team_id,
        concat("hometeam_placename_default",' ',"hometeam_commonname_default") as home_team_full_name,
        {{ string_to_int("hometeam_sog") }} as home_sog,
        {{ string_to_int("awayteam_sog") }} as away_sog,
        {{ string_to_int("hometeam_score") }} as home_score,
        {{ string_to_int("awayteam_score") }} as away_score
      from source
  ),
dup as (
    {{ dbt_utils.deduplicate(
        relation='renamed',
        partition_by='game_id',
        order_by= 'game_id'
        )
    }}
)
select * from dup order by game_id desc
