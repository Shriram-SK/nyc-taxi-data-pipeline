/*
  agg_zone_performance.sql
  ------------------------
  Zone-level rollup of trip volume, revenue, and operational performance.

  Source: int_trips_enriched (pre-filtered, zone-enriched trips)

  Grain: one row per pickup zone name

  Use cases
    - Identify highest-revenue and highest-volume pickup zones
    - Inform driver positioning and demand forecasting
    - Flag under-served vs. saturated zones

  Notes
    - Zones with NULL pickup_zone_name (TLC location IDs 264/265) are
      grouped under 'Unknown' rather than excluded, so revenue totals
      remain accurate and the NULL group is visible in reports.
    - revenue_rank uses RANK() not ROW_NUMBER() so zones with identical
      revenue receive the same rank (relevant for ties in lower-volume
      time slices).
    - is_high_volume_zone threshold (100,000 trips) is appropriate for a
      full year of 2023 data; revisit if the model is scoped to shorter
      periods.

  Materialization: table (inherited from dbt_project.yml → models.nyc_taxi.marts)
*/

with zone_stats as (

    select
        -- Coalesce so unknown zones appear as a labelled group rather
        -- than disappearing from reports that filter out NULLs
        coalesce(pickup_zone_name, 'Unknown') as pickup_zone_name,
        pickup_borough,

        -- Volume
        count(*)                              as total_trips,

        -- Revenue
        round(sum(revenue), 2)                as total_revenue,
        round(avg(revenue), 2)                as avg_revenue_per_trip,

        -- Distance
        round(avg(trip_distance), 3)          as avg_trip_distance,

        -- Operational
        round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
        coalesce(round(avg(trip_speed_mph), 2), 0)        as avg_speed_mph,
        round(sum(tip_amount), 2)             as total_tips,

        -- Earliest pickup month in the group — useful when this model is
        -- filtered to a sub-annual window and consumers need a period label
        date_trunc('month', min(pickup_date)) as sample_month,

        -- Tip rate as a percentage of total revenue — proxy for
        -- customer satisfaction and card vs. cash payment mix
        round(
            sum(tip_amount) / nullif(sum(revenue), 0) * 100,
            2
        )                                     as tip_rate_pct

    from {{ ref('int_trips_enriched') }}

    group by
        coalesce(pickup_zone_name, 'Unknown'),
        pickup_borough

),

ranked as (

    select
        pickup_zone_name,
        pickup_borough,
        total_trips,
        total_revenue,
        avg_revenue_per_trip,
        avg_trip_distance,
        avg_duration_minutes,
        avg_speed_mph,
        total_tips,
        sample_month,
        tip_rate_pct,

        -- Revenue rank across all zones (1 = highest revenue zone).
        -- RANK() assigns equal rank to ties; gaps appear after tied ranks.
        rank() over (order by total_revenue desc) as revenue_rank,

        -- Volume flag: zones with > 100k trips drive the majority of
        -- operational decisions (driver supply, pricing, expansion).
        total_trips > 100000                      as is_high_volume_zone

    from zone_stats

)

select * from ranked
order by revenue_rank
