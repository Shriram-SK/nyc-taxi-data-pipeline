-- ============================================================
-- q3_consecutive_gap_analysis.sql
-- ============================================================
-- Question: Within each pickup zone, how much time passes between
--           consecutive trips? Identifies demand surges and lulls.
--
-- Approach
--   LAG(pickup_datetime) partitioned by pickup_zone_name gives the
--   start time of the previous trip in that same zone. The difference
--   between that and the current trip's pickup is the inter-trip gap.
--
--   Partitioning by zone (not vendor) answers the operational question:
--   "how long is a zone going unserved?" rather than tracking individual
--   driver idle time (which q3 originally addressed with vendor_id).
--
-- Source : intermediate.int_trips_enriched
--   Note : dbt-duckdb may create this as 'main_intermediate'
--          depending on the generate_schema_name macro in use.
--          Adjust the schema prefix to match your environment.
--
-- Run    : duckdb nyc_taxi.duckdb < queries/q3_consecutive_gap_analysis.sql
-- ============================================================

with ordered as (

    select
        coalesce(pickup_zone_name, 'Unknown') as pickup_zone_name,
        pickup_datetime,

        -- Previous trip's pickup time within the same zone, ordered
        -- chronologically. NULL for the very first trip in each zone.
        lag(pickup_datetime) over (
            partition by coalesce(pickup_zone_name, 'Unknown')
            order by pickup_datetime
        )                                     as previous_trip_time

    from intermediate.int_trips_enriched

)

select
    pickup_zone_name,
    pickup_datetime,
    previous_trip_time,

    -- Gap in minutes between consecutive trips in this zone.
    -- Epoch subtraction preserves fractional minutes; round to 2 d.p.
    case 
        when previous_trip_time is not null 
        and pickup_datetime >= previous_trip_time
        then round(
                (epoch(pickup_datetime) - epoch(previous_trip_time)) / 60.0,
                2
            )
        end as gap_minutes

from ordered

-- Exclude the first trip per zone (no predecessor to compare against)
where previous_trip_time is not null

order by
    pickup_zone_name,
    pickup_datetime;
