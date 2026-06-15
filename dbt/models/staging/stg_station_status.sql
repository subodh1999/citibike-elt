{{ config(materialized='view') }}

-- One row per station per snapshot. data = raw JSON string of one station.
-- Booleans via CASE accepting BOTH '1'/'0' AND 'true'/'false' (plain SAFE_CAST would
-- NULL JSON booleans and silently break SCD2 change-tracking).
-- publish_time (TIMESTAMP) = authoritative SCD2 ordering key.

select
    json_value(data, '$.station_id')                                as station_id,
    safe_cast(json_value(data, '$.num_bikes_available')  as int64)  as num_bikes_available,
    safe_cast(json_value(data, '$.num_ebikes_available') as int64)  as num_ebikes_available,
    safe_cast(json_value(data, '$.num_docks_available')  as int64)  as num_docks_available,
    safe_cast(json_value(data, '$.num_bikes_disabled')   as int64)  as num_bikes_disabled,
    safe_cast(json_value(data, '$.num_docks_disabled')   as int64)  as num_docks_disabled,
    case when lower(json_value(data, '$.is_installed')) in ('1','true') then 1
         when lower(json_value(data, '$.is_installed')) in ('0','false') then 0 end as is_installed,
    case when lower(json_value(data, '$.is_renting'))   in ('1','true') then 1
         when lower(json_value(data, '$.is_renting'))   in ('0','false') then 0 end as is_renting,
    case when lower(json_value(data, '$.is_returning')) in ('1','true') then 1
         when lower(json_value(data, '$.is_returning')) in ('0','false') then 0 end as is_returning,
    safe_cast(json_value(data, '$.last_reported') as int64)         as last_reported,
    json_value(data, '$.ingest_ts')                                 as fetch_ingest_ts_raw,
    publish_time
from {{ source('raw', 'station_status_stream') }}