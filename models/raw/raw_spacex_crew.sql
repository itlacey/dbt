

{{ 
    config(
        materialized='table',
        schema='raw',
        tags='spacex'
        ) 
    
}}

with raw_crew as (


select 
replace((json_raw::json->'name')::varchar,'"','') name
,replace((json_raw::json->'agency')::varchar,'"','') agency
,replace((json_raw::json->'image')::varchar,'"','') image
,replace((json_raw::json->'wikipedia')::varchar,'"','') wikipedia
,replace((json_raw::json->'launches')::varchar,'"','') launches
,replace((json_raw::json->'status')::varchar,'"','') status
,replace((json_raw::json->'id')::varchar,'"','') id
, json_raw::json payload
, now() etl_dttm
from {{source('datalake','spacex_crew')}}
)

select *
from raw_crew