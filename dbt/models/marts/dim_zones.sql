/*
  dim_zones.sql
  -------------
  Zone dimension table for the NYC Taxi data mart.

  Source: stg_taxi_zones (255 TLC location reference rows)

  Provides the human-readable zone and borough lookup that BI tools
  join against fact tables. Sourced directly from staging rather than
  the intermediate layer because no trip-level filtering should affect
  the completeness of this dimension — all 255 zones must be present
  even if some have zero trips in the current period.

  Grain: one row per TLC location_id (primary key)

  Materialization: table (inherited from dbt_project.yml → models.nyc_taxi.marts)
*/

with source as (

    select * from {{ ref('stg_taxi_zones') }}

)

select
    -- Primary key — join target for pickup_location_id / dropoff_location_id
    location_id,

    -- Zone display name as provided by TLC
    zone,

    -- NYC borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
    borough,

    -- TLC service classification: 'Yellow Zone', 'Boro Zone', 'Airports', 'EWR'
    -- Useful for segmenting airport vs. city demand in dashboards
    service_zone

from source
