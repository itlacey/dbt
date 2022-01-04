
{{ 
    config(
        materialized='table',
        schema='raw',
        tags='spacex'
        ) 
    
}}

with raw_launch as (

   select 
json_raw::json->>'flight_number' flight_number
,json_raw::json->>'name' name
,(json_raw::json->>'date_utc')::timestamp date_utc
,(json_raw::json->>'date_unix')::int date_unix
,(json_raw::json->>'date_local')::timestamp  date_local
,json_raw::json->>'date_precision' date_precision
,(json_raw::json->>'auto_update')::boolean auto_update
,(json_raw::json->>'tbd')::boolean tbd
,json_raw::json->>'launch_library_id' launch_library_id
,json_raw::json->>'id' id
,(json_raw::json->>'static_fire_date_utc')::timestamp static_fire_date_utc
,(json_raw::json->>'static_fire_date_unix')::int static_fire_date_unix
,json_raw::json->>'net' net
,json_raw::json->>'window' window_
,json_raw::json->>'rocket' rocket
,(json_raw::json->>'success')::boolean success
,((json_raw::json->>'fairings')::json->>'reused')::boolean fairings_reused
,((json_raw::json->>'fairings')::json->>'recovery_attempt')::boolean fairings_recovery_attempt
,((json_raw::json->>'fairings')::json->>'recovered')::boolean fairings_recovered
,((json_raw::json->>'links')::json->>'patch')::json->>'large' links_patch
,((json_raw::json->>'links')::json->>'webcast') links_webcast
,((json_raw::json->>'links')::json->>'article') links_article
,((json_raw::json->>'links')::json->>'wikipedia') links_wikipedia
,(json_raw::json->>'crew') crew
,(json_raw::json->>'ships') ships
,(json_raw::json->>'capsules') capsules
,(json_raw::json->>'payloads') payloads
from {{source('spacex','launch')}}
)

select *
from raw_launch
