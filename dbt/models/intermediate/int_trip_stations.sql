{{ config(materialized='table') }}

-- Station dimension built from the TRIP data's own namespace (decimal ids like 2733.03),
-- NOT GBFS station_info (UUIDs) — the two namespaces have ZERO overlap (verified).
-- A station appears as both a start and an end across trips; union both sides, then
-- pick the most-recent name/coords per station_id (names occasionally get corrected).
-- City derived by the SAME lat/lon rule as stg_station_info: lon < -74.02 AND lat > 40.66 = jc.

with endpoints as (
    select
        start_station_id as station_id,
        start_station_name as station_name,
        start_lat as lat, start_lng as lon,
        started_at as seen_at
    from {{ ref('stg_trips') }}
    where start_station_id is not null

    union all

    select
        end_station_id as station_id,
        end_station_name as station_name,
        end_lat as lat, end_lng as lon,
        ended_at as seen_at
    from {{ ref('stg_trips') }}
    where end_station_id is not null
),

ranked as (
    select
        station_id, station_name, lat, lon,
        row_number() over (
            partition by station_id
            order by seen_at desc
        ) as rn
    from endpoints
    where lat is not null and lon is not null
)

select
    station_id,
    station_name,
    lat,
    lon,
    case when lon < -74.02 and lat > 40.66 then 'jc' else 'nyc' end as station_city
from ranked
where rn = 1