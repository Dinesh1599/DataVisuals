{{ config(materialized='table')}}

WITH src AS (
    SELECT * FROM {{ ref('stg_pit_stops') }}
)

SELECT
    race_id,
    driver_id,
    stop_number,
    lap,
    pit_time,
    duration,
    milliseconds
FROM src
