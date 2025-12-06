{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_constructors') }}
),

cleaned AS (
    SELECT
        "constructorId"::int AS constructor_id,
        TRIM(LOWER("constructorRef")) AS constructor_ref,
        TRIM("name") AS name,
        TRIM("nationality") AS nationality,
        "url",
        ROW_NUMBER() OVER (PARTITION BY "constructorId" ORDER BY "constructorId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
