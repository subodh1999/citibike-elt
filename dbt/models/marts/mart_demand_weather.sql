{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'activity_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['city', 'station_id']
) }}

-- GOLD: demand + weather, joined on city + hour. THE ML / Tableau feeder.
-- Built FROM mart_station_hourly (inherits station dim). Weather joined by matching
-- city and the truncated hour. Both clocks are NYC-local => alignment correct.
-- LEFT join: keep all demand rows even if a weather hour is missing.

with demand as (
    select *
    from {{ ref('mart_station_hourly') }}
    {% if is_incremental() %}
    where activity_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

weather as (
    select city, weather_hour, temp_c, humidity_pct, precip_mm, wind_kmh, snow_cm
    from {{ ref('stg_weather') }}
)

select
    d.city,
    d.station_id,
    d.station_name,
    d.lat,
    d.lon,
    d.activity_hour,
    d.activity_date,
    d.hour_of_day,
    d.day_of_week,
    d.is_weekend,
    d.departures,
    d.arrivals,
    d.avg_departure_duration_min,
    d.member_departures,
    d.casual_departures,
    w.temp_c,
    w.humidity_pct,
    w.precip_mm,
    w.wind_kmh,
    w.snow_cm
from demand d
left join weather w
    on  d.city = w.city
    and d.activity_hour = w.weather_hour