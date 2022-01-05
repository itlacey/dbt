
{{ 
    config(
        materialized='table',
        schema='raw',
        tags='spacex'
        ) 
    
}}

with raw_capsules as (

select 
(json_raw::json->'reuse_count')::varchar::int reuse_count
,(json_raw::json->'water_landings')::varchar::int water_landings
,(json_raw::json->'land_landings')::varchar::int land_landings
,replace((json_raw::json->'last_update')::varchar ,'"','')last_update
,replace((json_raw::json->'launches')::varchar,'"','') launches
,replace((json_raw::json->'serial')::varchar,'"','') serial
,replace((json_raw::json->'status')::varchar,'"','') status
,replace((json_raw::json->'type')::varchar,'"','') type
,replace((json_raw::json->'id')::varchar,'"','') id
,json_raw::json payload
, now() etl_dttm
from {{source('datalake','spacex_capsules')}}
)

select *
from raw_capsules