with source as (
        select * from {{ source('nhl_raw', 'raw_teams') }}
  ),
  renamed as (
      select
      "tricode" as team_id,
      {{ string_to_int('id') }} as team_code,
      "fullname" as full_name
      from source
  )
  select * from renamed
