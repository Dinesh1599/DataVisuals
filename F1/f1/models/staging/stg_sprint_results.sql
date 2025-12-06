{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_sprint_results') }}
),

cleaned AS (
    SELECT
        "resultId"::bigint AS result_id,
        "raceId"::bigint AS race_id,
        NULLIF(TRIM("driverId"::text), '\N')::bigint AS driver_id,
        NULLIF(TRIM("constructorId"::text), '\N')::bigint AS constructor_id,
        NULLIF(TRIM("number"::text), '\N')::bigint AS car_number,
        NULLIF(TRIM("grid"::text), '\N')::bigint AS grid_position,
        NULLIF(TRIM("position"::text), '\N')::int AS position,
        NULLIF(TRIM("positionText"), '\N') AS position_text,
        NULLIF(TRIM("positionOrder"::text), '\N')::int AS position_order,
        NULLIF(TRIM("points"::text), '\N')::int AS points,
        NULLIF(TRIM("laps"::text), '\N')::int AS laps,
        NULLIF(TRIM("time"::text), '\N') AS time,
        NULLIF(TRIM("milliseconds"::text), '\N')::bigint AS milliseconds,
        NULLIF(TRIM("fastestLap"::text), '\N')::int AS fastest_lap,
        NULLIF(TRIM("fastestLapTime"::text), '\N') AS fastest_lap_time,
        NULLIF(TRIM("statusId"::text), '\N')::int AS status_id,
        ROW_NUMBER() OVER (PARTITION BY "resultId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
