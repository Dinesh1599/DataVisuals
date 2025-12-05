{{ config(materialized='table') }}

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
        NULLIF(TRIM("q1"), '\N') AS q1,
        NULLIF(TRIM("q2"), '\N') AS q2,
        NULLIF(TRIM("q3"), '\N') AS q3,
        ROW_NUMBER() OVER (PARTITION BY "qualifyId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1;
