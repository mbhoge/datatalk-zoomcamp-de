{{ config(materialized='view') }}

select * from {{ source('staging', 'green_taxi_trip_data')}}