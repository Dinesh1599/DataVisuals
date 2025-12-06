{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_results') }}
),

cleaned AS (
    SELECT
        "resultId"::int AS result_id,
        "raceId"::int AS race_id,
        "driverId"::int AS driver_id,
        "constructorId"::int AS constructor_id,
        NULLIF(NULLIF(TRIM("number"), '\N'), '')::int AS car_number,
        NULLIF(NULLIF(TRIM("grid"::text), '\N'), '')::int AS grid_position,
        NULLIF(NULLIF(TRIM("position"), '\N'), '')::int AS position,
        NULLIF(NULLIF(TRIM("positionText"), '\N'), '') AS position_text,
        NULLIF(NULLIF(TRIM("positionOrder"::text), '\N'), '')::int AS position_order,
        NULLIF(NULLIF(TRIM("points"::text), '\N'), '')::float AS points,
        NULLIF(NULLIF(TRIM("laps"::text), '\N'), '')::int AS laps,
        NULLIF(NULLIF(TRIM("time"), '\N'), '') AS time,
        NULLIF(NULLIF(TRIM("milliseconds"), '\N'), '')::bigint AS milliseconds,
        NULLIF(NULLIF(TRIM("fastestLap"), '\N'), '')::int AS fastest_lap,
        NULLIF(NULLIF(TRIM("rank"), '\N'), '')::int AS rank,
        NULLIF(NULLIF(TRIM("fastestLapTime"), '\N'), '') AS fastest_lap_time,
        NULLIF(NULLIF(TRIM("fastestLapSpeed"), '\N'), '') AS fastest_lap_speed,
        NULLIF(NULLIF(TRIM("statusId"::text), '\N'), '')::int AS status_id,

        ROW_NUMBER() OVER (PARTITION BY "resultId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1

