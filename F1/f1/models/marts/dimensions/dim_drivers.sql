{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_drivers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['driver_id']) }} AS driver_sk,
    driver_id,
    driver_ref,
    code,
    driver_number,
    forename,
    surname,
    dob,
    nationality,
    url
FROM src
  