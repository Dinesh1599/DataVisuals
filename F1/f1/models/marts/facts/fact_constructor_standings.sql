{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_constructor_standings') }}
)

SELECT
    constructor_standings_id,
    race_id,
    constructor_id,
    points,
    position,
    wins
FROM src
