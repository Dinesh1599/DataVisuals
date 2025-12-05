{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_results') }}
),

cleaned AS (
    SELECT
        "resultId"::int AS result_id,
        NULLIF(TRIM("raceId"), '\N')::int AS race_id,
        NULLIF(TRIM("driverId"), '\N')::int AS driver_id,
        NULLIF(TRIM("constructorId"), '\N')::int AS constructor_id,
        NULLIF(TRIM("number"), '\N')::int AS car_number,
        NULLIF(TRIM("grid"), '\N')::int AS grid_position,
        NULLIF(TRIM("position"), '\N')::int AS position,
        NULLIF(TRIM("positionText"), '\N') AS position_text,
        NULLIF(TRIM("positionOrder"), '\N')::int AS position_order,
        NULLIF(TRIM("points"), '\N')::float AS points,
        NULLIF(TRIM("laps"), '\N')::int AS laps,
        NULLIF(TRIM("time"), '\N') AS finish_time,
        NULLIF(TRIM("milliseconds"), '\N')::bigint AS milliseconds,
        NULLIF(TRIM("fastestLap"), '\N')::int AS fastest_lap,
        NULLIF(TRIM("rank"), '\N')::int AS rank,
        NULLIF(TRIM("fastestLapTime"), '\N') AS fastest_lap_time,
        NULLIF(TRIM("fastestLapSpeed"), '\N') AS fastest_lap_speed,
        NULLIF(TRIM("statusId"), '\N')::int AS status_id,
        ROW_NUMBER() OVER (PARTITION BY "resultId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1;
