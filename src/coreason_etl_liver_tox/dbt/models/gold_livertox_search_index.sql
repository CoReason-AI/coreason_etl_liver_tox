{{ config(materialized='view') }}

SELECT
    agent_name,
    hepatotoxicity_summary
FROM {{ ref('silver_livertox_records') }}
WHERE agent_name IS NOT NULL
  AND hepatotoxicity_summary IS NOT NULL
