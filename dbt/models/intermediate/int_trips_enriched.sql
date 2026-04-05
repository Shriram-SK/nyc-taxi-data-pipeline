/*
  int_trips_enriched.sql
  ----------------------
  Intermediate model: cleaned, zone-enriched trip records ready for
  aggregation in the marts layer.

  Sources
    stg_yellow_trips  — one row per raw trip, typed and keyed
    stg_taxi_zones    — 255 TLC zone reference rows

  Responsibilities
    1. Join zone names and boroughs onto pickup and dropoff location IDs
    2. Apply data-quality filters to remove structurally invalid trips
    3. Derive is_long_trip and revenue convenience columns

  Assumptions
    - A trip with no matching zone is kept (LEFT JOIN); zone columns will
      be NULL for unknown location IDs (265 = "Unknown", 264 = "NV").
      Excluding these would silently under-count revenue — let mart-layer
      consumers decide how to handle them.
    - Filters are intentionally conservative. Edge cases (e.g. very short
      paid trips at airports) are preserved; anomaly detection belongs in
      dbt tests, not hard-coded WHERE clauses.
    - trip_duration_minutes is already null-safe from the staging model.
      NULL values fail the BETWEEN predicate and are excluded without an
      explicit IS NOT NULL check.

  Materialization: table (inherited from dbt_project.yml → models.nyc_taxi.intermediate)
*/

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

zones as (

    select * from {{ ref('stg_taxi_zones') }}

),

enriched as (

    select

        -- ----------------------------------------------------------------
        -- Identifiers & time dimensions
        -- ----------------------------------------------------------------
        trips.trip_id,
        trips.vendor_id,
        trips.pickup_datetime,
        trips.dropoff_datetime,
        trips.pickup_date,

        -- ----------------------------------------------------------------
        -- Geography — raw IDs retained for joins; human-readable names
        -- added from the zone reference table.
        -- LEFT JOIN means these four columns are NULL when location_id
        -- has no match in stg_taxi_zones (e.g. ID 264/265).
        -- ----------------------------------------------------------------
        trips.pickup_location_id,
        pickup_zone.zone    as pickup_zone_name,
        pickup_zone.borough as pickup_borough,

        trips.dropoff_location_id,
        dropoff_zone.zone    as dropoff_zone_name,
        dropoff_zone.borough as dropoff_borough,

        -- ----------------------------------------------------------------
        -- Trip attributes
        -- ----------------------------------------------------------------
        trips.passenger_count,
        trips.trip_distance,
        trips.trip_duration_minutes,
        trips.rate_code_id,
        trips.payment_type,

        -- ----------------------------------------------------------------
        -- Fare components — kept individually so mart models can build
        -- any aggregation without re-reading the staging layer.
        -- ----------------------------------------------------------------
        trips.fare_amount,
        trips.tip_amount,
        trips.tolls_amount,
        trips.congestion_surcharge,
        trips.airport_fee,
        trips.total_amount,

        -- ----------------------------------------------------------------
        -- Derived columns
        -- ----------------------------------------------------------------

        -- Convenience flag for segmenting long vs. short trips in reports.
        -- Safe to evaluate here: trip_duration_minutes is never NULL in
        -- this result set because the BETWEEN filter below excludes NULLs.
        trips.trip_duration_minutes > 60 as is_long_trip,

        -- Alias total_amount as revenue for clarity in mart aggregations.
        -- No transformation — single source of truth for monetary totals.
        -- coalesce guards against rare NULL total_amount values in source data
        coalesce(trips.total_amount, 0)                                    as revenue,

        -- Calendar month of pickup — used for monthly rollup aggregations
        date_trunc('month', trips.pickup_date)                             as trip_month,

        -- Speed in mph derived from distance and duration.
        -- CASE guard is redundant given the BETWEEN filter above but kept
        -- for clarity; returns NULL when duration is zero or negative.
        case
            when trips.trip_duration_minutes > 0
                then trips.trip_distance / (trips.trip_duration_minutes / 60.0)
        end                                                                as trip_speed_mph

    from trips

    -- ----------------------------------------------------------------
    -- Zone enrichment joins
    -- LEFT JOIN is deliberate: unknown location IDs (264, 265) should
    -- not cause trip rows to disappear from downstream revenue counts.
    -- An INNER JOIN here would silently drop ~0.1% of real trips.
    -- ----------------------------------------------------------------
    left join zones as pickup_zone
        on trips.pickup_location_id = pickup_zone.location_id

    left join zones as dropoff_zone
        on trips.dropoff_location_id = dropoff_zone.location_id

    -- ----------------------------------------------------------------
    -- Data quality filters
    -- These remove records that are metering errors or test entries,
    -- not legitimate passenger trips. Thresholds are based on TLC
    -- data documentation and standard industry practice.
    --
    -- trip_distance > 0      : zero-distance trips are meter tests or
    --                          cancellations, not completed journeys.
    -- fare_amount > 0        : non-positive fares indicate voids,
    --                          disputes, or no-charge codes.
    -- passenger_count > 0    : driver-entered field; zero means it was
    --                          not set — unreliable for demand analysis.
    -- trip_duration_minutes  : < 1 min are almost always meter errors;
    --   BETWEEN 1 AND 180      > 3 hrs are extreme outliers that skew
    --                          averages. NULLs fail BETWEEN implicitly.
    -- ----------------------------------------------------------------
    where trips.trip_distance       > 0
      and trips.fare_amount         > 0
      and trips.passenger_count     > 0
      and trips.trip_duration_minutes between 1 and 180

)

select * from enriched
