
{{ 
    config(
        materialized='table',
        schema='raw',
        tags='health'
        ) 
    
}}

with raw_health_scale as (
    select 
replace((json_raw::json->'operationType')::varchar,'"','') operationType
,replace((json_raw::json->'entryTimestamp')::varchar,'"','')::timestamp entryTimestamp
,replace((json_raw::json->'serverTimestamp')::varchar,'"','')::timestamp serverTimestamp
,(json_raw::json->'weight')::varchar::int weight
,(json_raw::json->'bodyFat')::varchar bodyFat
,(json_raw::json->'muscleMass')::varchar muscleMass
,(json_raw::json->'water')::varchar water
,replace((json_raw::json->'source')::varchar,'"','') source
,(json_raw::json->'bmi')::varchar bmi
,json_raw::json payload
, now() etl_dttm
from {{source('datalake','health_scale')}}

)

select * from raw_health_scale