{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by int64_field_0, pickup_datetime) as rn
  from {{ source('staging','fhv_data') }}
  where int64_field_0 !=0
)

select
    {{dbt_utils.surrogate_key(['int64_field_0','pickup_datetime'])}} as id,
    int64_field_0 as field,
    dispatching_base_num as dispatching_base,
    pickup_datetime,
    dropoff_datetime,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    SR_Flag,
    Affiliated_base_number as Affiliated_base
from tripdata
where rn=1

{% if var('is_test_run',default=false )%}

    limit 100

{% endif %}