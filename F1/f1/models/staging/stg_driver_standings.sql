{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_driver_standings') }}
),

cleaned AS (
    SELECT
        "driverStandingsId"::int AS driver_standings_id,
        "raceId"::int AS race_id,
        "driverId"::int AS driver_id,
        "points"::float AS points,
        "position"::int AS position,
        TRIM("positionText") AS position_text,
        "wins"::int AS wins,
        ROW_NUMBER() OVER (PARTITION BY "driverStandingsId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
