
{{ 
    config(
        materialized='table',
        schema='raw',
        tags='health'
        ) 
    
}}

with raw_health_run as (
select 
(json_raw::json->'activityId')::varchar::int activityId
,replace((json_raw::json->'activityType')::varchar,'"','') activityType
,(json_raw::json->'duration')::varchar duration
,(json_raw::json->'distance')::varchar distance
,(json_raw::json->'calories')::varchar calories
,replace((json_raw::json->'notes')::varchar,'"','') notes
,replace((json_raw::json->'startDateTimeLocal')::varchar ,'"','')::timestamp startDateTimeLocal
,replace((json_raw::json->'externalId')::varchar,'"','') externalId
,replace((json_raw::json->'source')::varchar,'"','') source
,replace((json_raw::json->'appVersion')::varchar,'"','') appVersion
,replace((json_raw::json->'deviceType')::varchar ,'"','')deviceType
,(json_raw::json->'hasDetails')::varchar hasDetails
,(json_raw::json->'hasDetailsGPS')::varchar hasDetailsGPS
,(json_raw::json->'startLatitude')::varchar::float startLatitude
,(json_raw::json->'startLongitude')::varchar::float startLongitude
,(json_raw::json->'heartRateMax')::varchar::int heartRateMax
,(json_raw::json->'heartRateMin')::varchar::int heartRateMin
,(json_raw::json->'heartRateAverage')::varchar::int heartRateAverage
,(json_raw::json->'cadenceMax')::varchar::int cadenceMax
,(json_raw::json->'cadenceMin')::varchar::int cadenceMin
,(json_raw::json->'cadenceAverage')::varchar::int cadenceAverage
,(json_raw::json->'temperature')::varchar::decimal temperature
,replace((json_raw::json->'howFelt')::varchar,'"','') howFelt
,replace((json_raw::json->'terrain')::varchar,'"','') terrain
,(json_raw::json->'isRace')::varchar isRace
,(json_raw::json->'isTreadmill')::varchar isTreadmill
,(json_raw::json->'isBadHR')::varchar isBadHR
,(json_raw::json->'isBadElevation')::varchar isBadElevation
,replace((json_raw::json->'syncDateTimeUTC')::varchar,'"','')::timestamp syncDateTimeUTC
,replace((json_raw::json->'dateCreatedUTC')::varchar,'"','')::timestamp dateCreatedUTC
,replace((json_raw::json->'dateUpdatedUTC')::varchar,'"','')::timestamp dateUpdatedUTC
,json_raw::json json_payload
,now() etl_dttm
from {{source('datalake','health_run')}}

)

select * from raw_health_run