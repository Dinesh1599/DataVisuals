{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_seasons') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['season_year']) }} AS season_sk,
    season_year,
    url
FROM src
