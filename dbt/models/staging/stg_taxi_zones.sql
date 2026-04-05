/*
  stg_taxi_zones.sql
  ------------------
  Staging model for NYC TLC taxi zone reference data.

  Source
    dbt seed: seeds/taxi_zone_lookup.csv
    255 rows — one per TLC location ID (covers all five boroughs + EWR/unknown).

  This layer is responsible for:
    1. Renaming PascalCase seed columns to snake_case
    2. Explicit type casts matching the seed column_types in dbt_project.yml
    3. A nullable boolean flag marking airport service zones for convenience
    4. A normalised zone name (zone_clean) for case-insensitive joins and display

  No filtering — all 255 zones are preserved so downstream joins never
  silently drop rows due to a missing zone.

  Materialization: view (inherited from dbt_project.yml → models.nyc_taxi.staging)
*/

with source as (

    -- ref() ensures dbt tracks the seed → model dependency
    -- and rebuilds this view whenever the seed is refreshed.
    select * from {{ ref('taxi_zone_lookup') }}

),

renamed as (

    select

        -- The TLC location ID — primary key, joins to pickup/dropoff
        -- location columns in stg_yellow_trips.
        cast(LocationID    as integer) as location_id,

        -- Administrative geography
        cast(Borough       as varchar) as borough,
        cast(Zone          as varchar) as zone,

        -- Lowercased zone name for case-insensitive joins and consistent
        -- display in downstream reports (TLC data has mixed casing).
        lower(cast(Zone    as varchar)) as zone_clean,

        -- TLC service zone classification:
        --   'Boro Zone', 'Yellow Zone', 'Airports', 'EWR'
        cast(service_zone  as varchar) as service_zone,

        -- Convenience flag: true for JFK, LGA, and EWR zones.
        -- Used in intermediate models to split airport vs. city trips.
        service_zone = 'Airports' as is_airport

    from source

)

select * from renamed
