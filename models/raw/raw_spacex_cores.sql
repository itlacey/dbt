
{{ 
    config(
        materialized='table',
        schema='raw',
        tags='spacex'
        ) 
    
}}

with raw_cores as (


select 
replace((json_raw::json->'block')::varchar,'"','') block
,replace((json_raw::json->'reuse_count')::varchar,'"','')::int reuse_count
,replace((json_raw::json->'rtls_attempts')::varchar,'"','')::int rtls_attempts
,replace((json_raw::json->'rtls_landings')::varchar,'"','')::int rtls_landings
,replace((json_raw::json->'asds_attempts')::varchar,'"','')::int asds_attempts
,replace((json_raw::json->'asds_landings')::varchar,'"','')::int asds_landings
,replace((json_raw::json->'last_update')::varchar,'"','') last_update
,replace((json_raw::json->'launches')::varchar,'"','') launches
,replace((json_raw::json->'serial')::varchar,'"','') serial
,replace((json_raw::json->'status')::varchar,'"','') status
,replace((json_raw::json->'id')::varchar,'"','') id
, json_raw::json payload
, now() etl_dttm
from {{source('datalake','spacex_cores')}}
)

select *
from raw_cores