{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_driver_standings') }}
)

SELECT
    driver_standings_id,
    race_id,
    driver_id,
    points,
    position,
    wins
FROM src

