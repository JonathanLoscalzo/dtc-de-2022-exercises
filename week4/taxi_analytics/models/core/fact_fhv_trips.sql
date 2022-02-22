{{ config(materialized='table') }}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    pk_fact_fhv_trips, 
    index, 
    dispatching_base_num, 
    pickup_datetime, 
    dropoff_datetime, 
    pulocationid, 
    dolocationid, 
    sr_flag, 
    
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 

    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone

from fhv_tripdata fhv
inner join dim_zones as pickup_zone
on fhv.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.dolocationid = dropoff_zone.locationid
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

