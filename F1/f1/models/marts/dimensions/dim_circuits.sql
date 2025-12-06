{{ config(materialized='table')}}

WITH src AS (
    SELECT * FROM {{ ref('stg_circuits') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['circuit_id']) }} AS circuit_sk,
    circuit_id,
    circuit_ref,
    name,
    location,
    country,
    lat,
    lng,
    altitude,
    url
FROM src
