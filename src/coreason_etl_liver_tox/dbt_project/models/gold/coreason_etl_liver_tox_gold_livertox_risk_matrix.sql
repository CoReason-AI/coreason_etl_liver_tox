{{ config(
    materialized='view'
) }}

SELECT
    agent_name,
    dili_likelihood_score
FROM {{ ref('coreason_etl_liver_tox_silver_livertox_records') }}
