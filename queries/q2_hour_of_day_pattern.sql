-- ============================================================
-- q2_hour_of_day_pattern.sql
-- ============================================================
-- Question: How many trips occur in each hour of the day (0–23)?
--           Useful for identifying demand peaks and quiet periods.
--
-- Source : intermediate.int_trips_enriched
--   Note : dbt-duckdb may create this as 'main_intermediate'
--          depending on the generate_schema_name macro in use.
--          Adjust the schema prefix to match your environment.
--
-- Run    : duckdb nyc_taxi.duckdb < queries/q2_hour_of_day_pattern.sql
-- ============================================================

with hours as (
    select * from generate_series(0, 23) as hour_of_day
),

trips as (
    select
        cast(extract(hour from pickup_datetime) as integer) as hour_of_day,
        count(*) as total_trips
    from intermediate.int_trips_enriched
    group by hour_of_day
)

select
    h.hour_of_day,
    coalesce(t.total_trips, 0) as total_trips
from hours h
left join trips t
    on h.hour_of_day = t.hour_of_day
order by h.hour_of_day;