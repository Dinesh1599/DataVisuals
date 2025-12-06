{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_sprint_results') }}
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
    fastest_lap_time
FROM src
