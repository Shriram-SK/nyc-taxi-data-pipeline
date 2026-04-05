/*
  agg_daily_metrics.sql
  ---------------------
  Daily rollup of trip volume and revenue metrics.

  Source: int_trips_enriched (pre-filtered, zone-enriched trips)

  Grain: one row per pickup_date

  Use cases
    - Daily revenue trend charts
    - Trip volume monitoring and anomaly alerting
    - Average fare and distance tracking over time

  Notes
    - avg_fare uses fare_amount (the metered component) rather than
      total_amount / revenue so that tip and surcharge volatility does
      not obscure the base fare trend.
    - All source rows have already passed quality filters in the
      intermediate layer; no additional filtering needed here.
    -- Note: date range is controlled upstream via dbt vars (start_date, end_date)
    -- ensuring consistent filtering across all models in the pipeline

  Materialization: table (inherited from dbt_project.yml → models.nyc_taxi.marts)
*/

with daily as (

    select
        pickup_date,

        -- Volume
        count(*)                        as total_trips,

        -- Revenue — uses the coalesced revenue column from int_trips_enriched
        -- so SUM is safe without an additional null check
        round(sum(revenue), 2)          as total_revenue,

        -- Average metered fare per trip (excludes tips, tolls, surcharges)
        round(avg(fare_amount), 2)      as avg_fare,

        -- Average trip distance in miles
        round(avg(trip_distance), 3)    as avg_distance,

        -- Additional operational metrics useful for daily monitoring
        round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
        round(sum(tip_amount), 2)             as total_tips,
        round(avg(passenger_count), 2)        as avg_passengers

    from {{ ref('int_trips_enriched') }}

    group by pickup_date

)

select * from daily
order by pickup_date
