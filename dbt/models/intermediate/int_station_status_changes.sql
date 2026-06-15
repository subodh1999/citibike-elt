{{ config(materialized='table') }}

-- SCD2-style change detection on the station status stream.
-- Keep a snapshot ONLY when it's the first for a station OR a tracked field changed
-- vs the previous snapshot (ordered by publish_time). Kills the every-5-min identical
-- rows, preserves the trend.
-- TABLE (not incremental): LAG needs each station's full prior history. An incremental
-- run can't see prior state across batch boundaries => false "change" rows. Stream is
-- small, so a full rebuild is correct and cheap.

with ordered as (
    select
        station_id,
        publish_time,
        num_bikes_available,
        num_ebikes_available,
        num_docks_available,
        num_bikes_disabled,
        num_docks_disabled,
        is_renting,
        is_returning,
        row_number()              over w as rn,
        lag(num_bikes_available)  over w as prev_bikes,
        lag(num_ebikes_available) over w as prev_ebikes,
        lag(num_docks_available)  over w as prev_docks,
        lag(num_bikes_disabled)   over w as prev_bikes_disabled,
        lag(num_docks_disabled)   over w as prev_docks_disabled,
        lag(is_renting)           over w as prev_renting,
        lag(is_returning)         over w as prev_returning
    from {{ ref('stg_station_status') }}
    where station_id is not null
    window w as (partition by station_id order by publish_time)
)

select
    station_id,
    publish_time,
    num_bikes_available,
    num_ebikes_available,
    num_docks_available,
    num_bikes_disabled,
    num_docks_disabled,
    is_renting,
    is_returning
from ordered
where rn = 1                                                  -- first snapshot per station
   or num_bikes_available  is distinct from prev_bikes
   or num_ebikes_available is distinct from prev_ebikes
   or num_docks_available  is distinct from prev_docks
   or num_bikes_disabled   is distinct from prev_bikes_disabled
   or num_docks_disabled   is distinct from prev_docks_disabled
   or is_renting           is distinct from prev_renting
   or is_returning         is distinct from prev_returning