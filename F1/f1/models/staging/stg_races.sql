{{ config(materialized='view') }}

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
        {{f1.handle_time_cast("time")}} AS race_time,  --has null values
        "url",
        NULLIF(TRIM("fp1_date"), '\N')::date AS fp1_date,
        {{f1.handle_time_cast("fp1_time")}} AS fp1_time,
        NULLIF(TRIM("fp2_date"), '\N')::date AS fp2_date,
        {{f1.handle_time_cast("fp2_time")}} AS fp2_time,
        NULLIF(TRIM("fp3_date") , '\N')::date AS fp3_date,
        {{f1.handle_time_cast("fp3_time")}} AS fp3_time,
        NULLIF(TRIM("quali_date"), '\N')::date AS quali_date,
        {{f1.handle_time_cast("quali_time")}} AS quali_time,
        NULLIF(TRIM("sprint_date"), '\N')::date AS sprint_date,
        {{f1.handle_time_cast("sprint_time")}} AS sprint_time,
        ROW_NUMBER() OVER (PARTITION BY "raceId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
