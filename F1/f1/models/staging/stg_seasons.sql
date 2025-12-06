{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_seasons') }}
),

cleaned AS (
    SELECT
        "year"::int AS season_year,
        "url",
        ROW_NUMBER() OVER (PARTITION BY "year") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
