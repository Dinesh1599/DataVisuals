{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_constructors') }}
)

SELECT
    {{dbt_utils.generate_surrogate_key(['constructor_id'])}} AS constructor_sk,
    constructor_id,
    constructor_ref,
    name,
    nationality,
    url
FROM src
