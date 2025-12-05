{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_constructor_standings') }}
),

cleaned AS (
    SELECT
        "constructorStandingsId"::int AS constructor_standings_id,
        "raceId"::int AS race_id,
        "constructorId"::int AS constructor_id,
        "points"::float AS points,
        "position"::int AS position,
        TRIM("positionText") AS position_text,
        "wins"::int AS wins,
        ROW_NUMBER() OVER (PARTITION BY "constructorStandingsId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
