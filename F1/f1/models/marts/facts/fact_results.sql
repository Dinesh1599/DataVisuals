{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_results') }}
)

SELECT
    result_id,
    race_id,
    driver_id,
    constructor_id,
    status_id,
    grid_position,
    position,
    position_order,
    points,
    laps,
    time,
    milliseconds,
    fastest_lap,
    rank,
    fastest_lap_time,
    fastest_lap_speed
FROM src
