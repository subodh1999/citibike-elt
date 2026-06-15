{{ config(materialized='view') }}

-- One row per city per hour. time is STRING "YYYY-MM-DDTHH:MM", now NYC-local.
-- SAFE.PARSE_TIMESTAMP returns a TIMESTAMP. NO 'AT TIME ZONE' (invalid in BigQuery).

select
    city,
    latitude,
    longitude,
    timestamp_trunc(
        safe.parse_timestamp('%Y-%m-%dT%H:%M', time),
        hour
    ) as weather_hour,
    temperature_2m        as temp_c,
    relative_humidity_2m  as humidity_pct,
    precipitation         as precip_mm,
    wind_speed_10m        as wind_kmh,
    snowfall              as snow_cm,
    ingest_ts
from {{ source('raw', 'weather') }}