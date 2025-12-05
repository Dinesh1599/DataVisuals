{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_races') }}
),

cleaned AS (
    SELECT
        "raceId"::int AS race_id,
        "year"::int AS race_year,
        "round"::int AS round,
        "circuitId"::int AS circuit_id,
        TRIM("name") AS name,
        "date"::date AS race_date,
        "time" AS race_time,
        "url",
        NULLIF(TRIM("fp1_date")::date, '\N') AS fp1_date,
        NULLIF(TRIM("fp1_time"), '\N') AS fp1_time,
        NULLIF(TRIM("fp2_date")::date, '\N') AS fp2_date,
        NULLIF(TRIM("fp2_time"), '\N') AS fp2_time,
        NULLIF(TRIM("fp3_date") ::date, '\N') AS fp3_date,
        NULLIF(TRIM("fp3_time"), '\N') AS fp3_time,
        NULLIF(TRIM("quali_date")::date, '\N') AS quali_date,
        NULLIF(TRIM("quali_time"), '\N') AS quali_time,
        NULLIF(TRIM("sprint_date")::date, '\N') AS sprint_date,
        NULLIF(TRIM("sprint_time"), '\N') AS sprint_time,
        ROW_NUMBER() OVER (PARTITION BY "raceId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1;
