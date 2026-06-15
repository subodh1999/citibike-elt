{{ config(materialized='view') }}

-- One row per trip. Rename + row-level derivations only. No joins, no agg.
-- started_at/ended_at already TIMESTAMP (Spark). city already path-tagged (Spark).
-- start_date passes through UNCHANGED so downstream incrementals partition-prune
-- the 92M raw.trips table.

select
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    city,
    trip_duration_sec,
    round(trip_duration_sec / 60.0, 2) as trip_duration_min,
    timestamp_trunc(started_at, hour)  as start_hour,
    extract(dayofweek from started_at) as day_of_week,      -- BQ: 1=Sun .. 7=Sat
    extract(dayofweek from started_at) in (1, 7) as is_weekend,
    member_casual = 'member' as is_member,
    start_date,                                             -- UNCHANGED (prune key)
    ingest_ts
from {{ source('raw', 'trips') }}
{% if var('dev_limit', true) %}
where start_date = date('{{ var("dev_date", "2025-05-01") }}')
{% endif %}