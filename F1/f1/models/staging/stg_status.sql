{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'RAW_status') }}
),

cleaned AS (
    SELECT
        "statusId"::bigint AS status_id,
        TRIM("status") AS status_text,
        ROW_NUMBER() OVER (PARTITION BY "statusId") AS rn
    FROM src
)

SELECT *
FROM cleaned
WHERE rn = 1
