{{ config(
    materialized='view'
) }}

SELECT
    agent_name,
    hepatotoxicity_summary,
    mechanism_of_injury
FROM {{ ref('silver_livertox_records') }}
