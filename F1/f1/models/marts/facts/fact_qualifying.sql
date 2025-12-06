{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_qualifying') }}
)

SELECT
    qualify_id,
    race_id,
    driver_id,
    constructor_id,
    car_number,
    position,
    q1,
    q2,
    q3
FROM src
