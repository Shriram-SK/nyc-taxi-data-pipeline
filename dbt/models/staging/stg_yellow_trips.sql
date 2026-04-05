/*
  stg_yellow_trips.sql
  --------------------
  Staging model for NYC TLC Yellow Taxi trip records.

  Source
    Raw Parquet files at: {{ var('input_dir') }}/yellow_tripdata_2023-*.parquet
    Schema reference: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

  This layer is responsible for:
    1. Reading raw Parquet via DuckDB's read_parquet() glob
    2. Renaming PascalCase / ambiguous TLC columns to snake_case
    3. Explicit type casts to enforce a stable contract downstream
    4. A surrogate primary key: trip_id (md5 of vendor + pickup_datetime + pickup_location)
    5. Two derived columns: pickup_date and trip_duration_minutes (null-safe)

  No business logic or filtering lives here — that belongs in intermediate/.

  NOTE on source():
    DuckDB reads Parquet files directly via a table-function, not through a
    registered database relation, so source('raw_taxi', 'yellow_trips')
    cannot be used as the FROM target. The logical source is still declared in
    sources.yml for documentation and lineage purposes.

  Materialization: view (inherited from dbt_project.yml → models.nyc_taxi.staging)
*/

with source as (

    select *
    from read_parquet('{{ var("input_dir") }}/yellow_tripdata_2023-*.parquet')

),

renamed as (

    select

        -- ----------------------------------------------------------------
        -- Surrogate primary key
        -- Hashes three fields that together uniquely identify a trip in
        -- the TLC dataset. A '-' separator prevents cross-field collisions
        -- (e.g. vendor=1, location=23 vs vendor=12, location=3).
        -- coalesce guards against null components producing a null hash.
        -- ----------------------------------------------------------------
        md5(
            coalesce(cast(VendorID             as varchar), '') || '-' ||
            coalesce(cast(tpep_pickup_datetime as varchar), '') || '-' ||
            coalesce(cast(PULocationID         as varchar), '')
        ) as trip_id,

        -- ----------------------------------------------------------------
        -- Identifiers
        -- ----------------------------------------------------------------
        cast(VendorID          as integer)  as vendor_id,

        -- ----------------------------------------------------------------
        -- Timestamps → explicit TIMESTAMP cast guards against Parquet files
        -- that encode datetimes as strings in older TLC exports.
        -- ----------------------------------------------------------------
        cast(tpep_pickup_datetime  as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- ----------------------------------------------------------------
        -- Derived date column — used heavily in downstream aggregations
        -- and as a partition key in intermediate/mart tables.
        -- ----------------------------------------------------------------
        cast(tpep_pickup_datetime as date)       as pickup_date,

        -- ----------------------------------------------------------------
        -- Trip attributes
        -- ----------------------------------------------------------------
        cast(passenger_count   as integer)  as passenger_count,
        cast(trip_distance     as double)   as trip_distance,
        cast(RatecodeID        as integer)  as rate_code_id,
        cast(store_and_fwd_flag as varchar) as store_and_fwd_flag,
        cast(PULocationID      as integer)  as pickup_location_id,
        cast(DOLocationID      as integer)  as dropoff_location_id,
        cast(payment_type      as integer)  as payment_type,

        -- ----------------------------------------------------------------
        -- Fare breakdown
        -- ----------------------------------------------------------------
        cast(fare_amount             as double) as fare_amount,
        cast(extra                   as double) as extra,
        cast(mta_tax                 as double) as mta_tax,
        cast(tip_amount              as double) as tip_amount,
        cast(tolls_amount            as double) as tolls_amount,
        cast(improvement_surcharge   as double) as improvement_surcharge,
        cast(total_amount            as double) as total_amount,
        cast(congestion_surcharge    as double) as congestion_surcharge,
        -- airport_fee was added to the schema mid-2022; coalesce handles
        -- older files where the column may be absent or null.
        cast(coalesce(airport_fee, 0) as double) as airport_fee,

        -- ----------------------------------------------------------------
        -- Computed: trip duration in minutes (fractional, 2 d.p.)
        -- Uses epoch arithmetic rather than datediff('minute', ...) to
        -- preserve sub-minute precision for short trips.
        -- Returns null if either timestamp is null rather than erroring
        -- or producing a nonsensical negative/zero value.
        -- ----------------------------------------------------------------
        case
            when tpep_pickup_datetime is null 
            or tpep_dropoff_datetime is null
            or tpep_dropoff_datetime < tpep_pickup_datetime
            then null
            else round(
                (
                    epoch(cast(tpep_dropoff_datetime as timestamp))
                    - epoch(cast(tpep_pickup_datetime  as timestamp))
                ) / 60.0,
                2
            )
        end as trip_duration_minutes

    from source

)

select * from renamed
