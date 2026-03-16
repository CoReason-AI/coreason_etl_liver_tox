{{ config(
    materialized='view'
) }}

SELECT
    agent_name,
    dili_likelihood_score
FROM {{ ref('silver_livertox_records') }}
