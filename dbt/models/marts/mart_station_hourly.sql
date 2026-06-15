{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'activity_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['city', 'station_id']
) }}

-- GOLD: trip activity per station per hour + station dimension.
-- Dim comes from int_trip_stations (trip namespace), NOT stg_station_info (GBFS UUIDs,
-- zero key overlap with trips). capacity not available from trip data => omitted here.

with hourly as (
    select *
    from {{ ref('int_trips_hourly') }}
    {% if is_incremental() %}
    where activity_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

stations as (
    select station_id, station_name, lat, lon
    from {{ ref('int_trip_stations') }}
)

select
    h.city,
    h.station_id,
    s.station_name,
    s.lat,
    s.lon,
    h.activity_hour,
    h.activity_date,
    h.departures,
    h.arrivals,
    h.avg_departure_duration_min,
    h.member_departures,
    h.casual_departures,
    extract(hour      from h.activity_hour) as hour_of_day,
    extract(dayofweek from h.activity_hour) as day_of_week,
    extract(dayofweek from h.activity_hour) in (1, 7) as is_weekend
from hourly h
left join stations s using (station_id)