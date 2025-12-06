{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_constructor_results') }}
),

cleaned AS (
    SELECT
        "constructorResultsId"::int AS constructor_results_id,
        "raceId"::int AS race_id,
        "constructorId"::int AS constructor_id,
        "points"::float AS points,
        NULLIF(TRIM("status"), '\N') AS status,
        ROW_NUMBER() OVER (PARTITION BY "constructorResultsId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
