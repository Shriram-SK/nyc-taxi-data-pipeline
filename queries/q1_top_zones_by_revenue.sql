-- ============================================================
-- q1_top_zones_by_revenue.sql
-- ============================================================
-- Question: Which 10 pickup zones generated the most revenue?
--
-- Source : intermediate.int_trips_enriched
--   Note : dbt-duckdb may create this as 'main_intermediate'
--          depending on the generate_schema_name macro in use.
--          Adjust the schema prefix to match your environment.
--
-- Run    : duckdb nyc_taxi.duckdb < queries/q1_top_zones_by_revenue.sql
-- ============================================================

select
    -- NULL pickup_zone_name means TLC location ID 264/265 (unknown zones).
    -- Labelled explicitly so they appear in results rather than being dropped.
    coalesce(pickup_zone_name, 'Unknown') as pickup_zone_name,

    round(sum(revenue), 2)               as total_revenue

from intermediate.int_trips_enriched

group by coalesce(pickup_zone_name, 'Unknown')

order by total_revenue desc, pickup_zone_name

limit 10;
