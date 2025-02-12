with source as (
        select * from {{ source('nhl_raw', 'raw_roster_season') }}
  ),
  renamed as (
      select
        {{ string_to_int ('id') }} as player_id,
        firstname_default as first_name,
        lastname_default as last_name,
        substring(filename from  12 for 3) as team_id,
        {{ string_to_int('right(filename, 8)') }} as season_id,
        positioncode ,
        sweaternumber as jersey_number,
        shootscatches as shoots_or_catches,
        birthdate as birth_date,
        birthcountry  as birth_country,
        birthcity_default as birth_city,
        {{ string_to_int ('heightincentimeters') }} as height_centimeters,
        {{ string_to_int ('heightininches') }} as height_inches,
        {{ string_to_int ('weightinkilograms') }} as weight_kg,
        {{ string_to_int ('weightinpounds') }} as weight_pounds,
        headshot as pic_url
      from source
  )
  select
  concat(player_id::text, season_id::text, team_id) as pk,
  *
  from renamed
