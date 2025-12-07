{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_qualifying') }}
),

cleaned AS (
    SELECT
        "qualifyId"::int AS qualify_id,
        "raceId"::int AS race_id,
        "driverId"::int AS driver_id,
        "constructorId"::int AS constructor_id,
        "number"::int AS car_number,
        "position"::int AS position,
        {{handle_time_cast("q1")}} AS q1,
        {{handle_time_cast("q2")}} AS q2,
        {{handle_time_cast("q3")}} AS q3,
    
        ROW_NUMBER() OVER (PARTITION BY "qualifyId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
