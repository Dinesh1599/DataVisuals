{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_races') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['race_id']) }} AS race_sk,
    race_id,
    race_year,
    round,
    circuit_id,
    name AS race_name,
    race_date,
    race_time,
    fp1_date,
    fp1_time,
    fp2_date,
    fp2_time,
    fp3_date,
    fp3_time,
    quali_date,
    quali_time,
    sprint_date,
    sprint_time,
    url
FROM src