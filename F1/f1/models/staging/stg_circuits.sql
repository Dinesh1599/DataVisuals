{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_circuits') }}
),

cleaned AS (
    SELECT
        "circuitId"::int            AS circuit_id,
        TRIM(LOWER("circuitRef"))   AS circuit_ref,
        TRIM("name")                AS name,
        TRIM("location")            AS location,
        TRIM("country")             AS country,
        "lat"::float                AS lat,
        "lng"::float                AS lng,
        "alt"::int                  AS altitude,
        "url",
        ROW_NUMBER() OVER (PARTITION BY "circuitId" ORDER BY "circuitId" DESC) AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
