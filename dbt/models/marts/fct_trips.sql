/*
  fct_trips.sql
  -------------
  Core fact table for the NYC Taxi data mart.

  Source: int_trips_enriched (one row per valid, zone-enriched trip)

  This model is a direct pass-through of the intermediate layer.
  No further transformation is applied here — all filtering, joining,
  and derivation has already happened upstream. The purpose of
  materialising this as a table is to give BI tools and ad-hoc queries
  a single, fast, pre-joined surface to query against.

  Grain: one row per trip (trip_id is the primary key)

  Columns of note
    trip_id            — md5 surrogate key from staging
    revenue            — coalesced total_amount; safe for SUM without null checks
    trip_month         — date_trunc('month') of pickup_date; partition helper
    trip_speed_mph     — derived; NULL for trips where duration = 0
    pickup_zone_name   — NULL for unknown TLC location IDs (264, 265)
    dropoff_zone_name  — same

  Materialization: table (inherited from dbt_project.yml → models.nyc_taxi.marts)
*/

select * from {{ ref('int_trips_enriched') }}
