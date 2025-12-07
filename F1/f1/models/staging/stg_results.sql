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
        {{f1.safe_cast("number", "int")}} AS car_number,
        {{f1.safe_cast("grid", "int")}} AS grid_position,
        {{f1.safe_cast("position", "int")}} AS position,
        {{f1.handle_null("'positionText'")}} AS position_text,
        {{f1.safe_cast('"positionOrder"', "int")}} AS position_order,
        {{f1.safe_cast("points", "float")}} AS points,
        {{f1.safe_cast("laps", "int")}} AS laps,
        NULLIF(NULLIF(TRIM("time"), '\N'), '') AS time,
        {{f1.safe_cast("milliseconds", "int")}} AS milliseconds,
        {{f1.safe_cast('"fastestLap"', "int")}} AS fastest_lap,
        {{f1.safe_cast("rank", "int")}} AS rank,
        {{f1.safe_cast('"fastestLapTime"',"time")}} AS fastest_lap_time,
        {{f1.safe_cast('"fastestLapSpeed"',"float")}} AS fastest_lap_speed,
        {{f1.safe_cast('"statusId"', "int")}} AS status_id,

        ROW_NUMBER() OVER (PARTITION BY "resultId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1

