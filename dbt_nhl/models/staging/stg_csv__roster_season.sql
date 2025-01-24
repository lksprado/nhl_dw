with source as (
        select * from {{ source('nhl_raw', 'raw_roster_season') }}
  ),
  renamed as (
      select
        {{ string_to_int ('"id"') }} as player_id,
        "firstName.default" as first_name,
        "lastName.default" as last_name,
        SUBSTRING("filename" FROM  12 FOR 3) AS team_id,
        {{ string_to_int('RIGHT("filename", 8)') }} AS season_id,
        "positionCode" ,
        "sweaterNumber" as jersey_number,
        "shootsCatches" as shoots_or_catches,
        "birthDate" as birth_date,
        "birthCountry"  as birth_country,
        "birthCity.default" as birth_city,
        {{ string_to_int ('"heightInCentimeters"') }} as height_centimeters,
        {{ string_to_int ('"heightInInches"') }} as height_inches,
        {{ string_to_int ('"weightInKilograms"') }} as weight_kg,
        {{ string_to_int ('"weightInPounds"') }} as weight_pounds,
        "headshot" as pic_url
      from source
  )
  select
  concat(player_id::text, season_id::text, team_id) as pk,
  *
  from renamed
