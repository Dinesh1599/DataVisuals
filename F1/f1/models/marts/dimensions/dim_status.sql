{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_status') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['status_id']) }} AS status_sk,
    status_id,
    status_text
FROM src
