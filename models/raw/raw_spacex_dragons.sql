


{{ 
    config(
        materialized='table',
        schema='raw',
        tags='spacex'
        ) 
    
}}

with raw_dragons as (


select 
replace((json_raw::json->'first_flight')::varchar,'"','')::date first_flight
,replace((json_raw::json->'name')::varchar,'"','') name
,replace((json_raw::json->'type')::varchar,'"','') type
,replace((json_raw::json->'active')::varchar,'"','') active
,replace((json_raw::json->'crew_capacity')::varchar,'"','')::int crew_capacity
,replace((json_raw::json->'sidewall_angle_deg')::varchar,'"','')::int sidewall_angle_deg
,replace((json_raw::json->'orbit_duration_yr')::varchar,'"','')::int orbit_duration_yr
,replace((json_raw::json->'dry_mass_kg')::varchar,'"','')::int dry_mass_kg
,replace((json_raw::json->'dry_mass_lb')::varchar,'"','')::int dry_mass_lb
,replace((json_raw::json->'wikipedia')::varchar,'"','') wikipedia
,replace((json_raw::json->'description')::varchar,'"','') description
,replace((json_raw::json->'id')::varchar,'"','') id
,(((json_raw::json->'heat_shield')::varchar)::json->>'material') heat_shield_material
,(((json_raw::json->'heat_shield')::varchar)::json->>'size_meters')::float heat_shield_size_meters
,(((json_raw::json->'heat_shield')::varchar)::json->>'temp_degrees')::int heat_shield_temp_degrees
,(((json_raw::json->'heat_shield')::varchar)::json->>'dev_partner') heat_shield_dev_partner
,(((json_raw::json->'launch_payload_mass')::varchar)::json->>'kg')::int launch_payload_mass_kg
,(((json_raw::json->'launch_payload_mass')::varchar)::json->>'lb')::int launch_payload_mass_lb
,(((json_raw::json->'launch_payload_vol')::varchar)::json->>'cubic_meters')::int launch_payload_vol_cubic_meters
,(((json_raw::json->'launch_payload_vol')::varchar)::json->>'cubic_feet')::int launch_payload_vol_cubic_feet
,(((json_raw::json->'return_payload_mass')::varchar)::json->>'kg')::int return_payload_mass_kg
,(((json_raw::json->'return_payload_mass')::varchar)::json->>'lb')::int return_payload_mass_lb
,(((json_raw::json->'return_payload_vol')::varchar)::json->>'cubic_meters')::int return_payload_vol_cubic_meters
,(((json_raw::json->'return_payload_vol')::varchar)::json->>'cubic_feet')::int return_payload_vol_cubic_feet
,((((json_raw::json->'pressurized_capsule')::varchar)::json->>'payload_volume')::json->>'cubic_meters')::int pressurized_capsule_payload_volume_cubic_meters
,((((json_raw::json->'pressurized_capsule')::varchar)::json->>'payload_volume')::json->>'cubic_feet')::int pressurized_capsule_payload_volume_cubic_feet
,((((json_raw::json->'trunk')::varchar)::json->>'trunk_volume')::json->>'cubic_meters')::int trunk_volume_cubic_meters
,((((json_raw::json->'trunk')::varchar)::json->>'trunk_volume')::json->>'cubic_feet')::int trunk_volume_cubic_feet
,((((json_raw::json->'trunk')::varchar)::json->>'cargo')::json->>'solar_array')::int trunk_cargo_solar_array
,(((json_raw::json->'trunk')::varchar)::json->>'cargo')::json->>'unpressurized_cargo' trunk_cargo_unpressurized_cargo
,(((json_raw::json->'height_w_trunk')::varchar)::json->>'meters')::float height_w_trunk_meters
,(((json_raw::json->'height_w_trunk')::varchar)::json->>'feet')::float height_w_trunk_feet
,(((json_raw::json->'diameter')::varchar)::json->>'meters')::float diameter_meters
,(((json_raw::json->'diameter')::varchar)::json->>'feet')::float diameter_feet
,replace((json_raw::json->'flickr_images')::varchar,'"','') flickr_images
,replace((json_raw::json->'thrusters')::varchar,'"','') thrusters
, json_raw::json payload
, now() etl_dttm
from {{source('datalake','spacex_dragons')}}
)

select *
from raw_dragons