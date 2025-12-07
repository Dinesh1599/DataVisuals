{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_drivers') }}
),

cleaned AS (
    SELECT
        "driverId"::int AS driver_id,
        TRIM(LOWER("driverRef")) AS driver_ref,
        {{safe_cast("number", 'int')}} AS driver_number,
        TRIM("code") AS code,
        TRIM("forename") AS forename,
        TRIM("surname") AS surname,
        "dob"::date AS dob,
        TRIM("nationality") AS nationality,
        "url",
        ROW_NUMBER() OVER (PARTITION BY "driverId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
--- IGNORE ---