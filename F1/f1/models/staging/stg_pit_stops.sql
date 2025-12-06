{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_pit_stops') }}
),

cleaned AS (
    SELECT
        "raceId"::bigint AS race_id,
        "driverId"::bigint AS driver_id,
        "stop"::int AS stop_number,
        "lap"::int AS lap,
        TRIM("time") AS pit_time,
        TRIM("duration") AS duration,
        "milliseconds"::bigint AS milliseconds,
        ROW_NUMBER() OVER (PARTITION BY "raceId", "driverId", "stop") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
--- IGNORE ---