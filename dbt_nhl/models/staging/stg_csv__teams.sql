with source as (
        select * from {{ source('nhl_raw', 'raw_teams') }}
  ),
  renamed as (
      select
      "triCode" as team_id,
      {{ string_to_int('id') }} as team_code,
      "fullName" as full_name
      from source
  )
  select * from renamed
