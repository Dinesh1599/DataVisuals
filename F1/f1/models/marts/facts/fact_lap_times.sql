{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_lap_times') }}
)

SELECT
    race_id,
    driver_id,
    lap,
    position,
    lap_time,
    milliseconds
FROM src
