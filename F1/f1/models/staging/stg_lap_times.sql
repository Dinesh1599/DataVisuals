{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_lap_times') }}
),

cleaned AS (
    SELECT
        "raceId"::int AS race_id,
        "driverId"::int AS driver_id,
        "lap"::int AS lap,
        "position"::int AS position,
        {{handle_time_cast("time")}} AS lap_time,
        "milliseconds"::int AS milliseconds,
        ROW_NUMBER() OVER (PARTITION BY "raceId", "driverId", "lap") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
