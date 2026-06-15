{{ config(materialized='view') }}

-- One row per station. City from lat/lon (region_id BANNED as city key).
-- Both axes needed: lon kills Manhattan, lat kills Bay Ridge (Brooklyn shares JC lon).
-- Verified vs raw.station_info: Brooklyn tops lat 40.6465, JC starts 40.6851.
--   JC = lon < -74.02 AND lat > 40.66   else NYC

select
    station_id,
    name,
    lat,
    lon,
    capacity,
    region_id,                                   -- carried, NOT used for city
    case
      when lon < -74.02 and lat > 40.66 then 'jc'
      else 'nyc'
    end as city,
    ingest_ts
from {{ source('raw', 'station_info') }}