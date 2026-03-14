{{ config(materialized='table') }}

SELECT
    agent_name,
    dili_likelihood_score
FROM {{ ref('silver_livertox_records') }}
WHERE agent_name IS NOT NULL
  AND dili_likelihood_score IS NOT NULL
