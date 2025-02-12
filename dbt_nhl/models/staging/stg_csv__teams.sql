with source as (
        select * from {{ source('nhl_raw', 'raw_teams') }}
  ),
  renamed as (
      select
      "tricode" as team_trid,
      {{ string_to_int('id') }} as team_id,
      "fullname" as full_name
      from source
  )
  select * from renamed
