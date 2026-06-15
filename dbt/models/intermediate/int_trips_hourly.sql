{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'activity_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['city', 'station_id']
) }}

-- Per city / station / hour trip activity. Each trip emits TWO events: a departure at
-- its start station+hour and an arrival at its end station+hour. Grouped to one row per
-- station per hour with departures, arrivals, and departure duration/rider mix.
-- Incremental + insert_overwrite, partitioned by activity_date (prunes the 92M table
-- via stg_trips.start_date). Demand target for ML = departures.

with trips as (
    select *
    from {{ ref('stg_trips') }}
    {% if is_incremental() %}
    where start_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

events as (
    -- departures
    select
        city,
        start_station_id                as station_id,
        start_hour                      as activity_hour,
        start_date                      as activity_date,
        1                               as is_departure,
        0                               as is_arrival,
        trip_duration_min,
        is_member
    from trips
    where start_station_id is not null

    union all

    -- arrivals
    select
        city,
        end_station_id                  as station_id,
        timestamp_trunc(ended_at, hour) as activity_hour,
        date(ended_at)                  as activity_date,
        0                               as is_departure,
        1                               as is_arrival,
        trip_duration_min,
        is_member
    from trips
    where end_station_id is not null
)

select
    city,
    station_id,
    activity_hour,
    activity_date,
    sum(is_departure)                                                    as departures,
    sum(is_arrival)                                                      as arrivals,
    round(avg(if(is_departure = 1, trip_duration_min, null)), 2)        as avg_departure_duration_min,
    sum(if(is_departure = 1 and is_member, 1, 0))                       as member_departures,
    sum(if(is_departure = 1 and not is_member, 1, 0))                   as casual_departures
from events
group by 1, 2, 3, 4