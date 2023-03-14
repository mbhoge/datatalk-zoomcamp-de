{{ config(materialized='view') }}

select VendorID from {{ source('staging', 'green_taxi_trip_data')}}
limit 100