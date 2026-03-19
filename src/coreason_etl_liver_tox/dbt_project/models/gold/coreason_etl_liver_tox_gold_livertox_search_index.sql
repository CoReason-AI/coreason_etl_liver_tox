{{ config(
    materialized='view'
) }}

SELECT
    agent_name,
    hepatotoxicity_summary,
    mechanism_of_injury
FROM {{ ref('coreason_etl_liver_tox_silver_livertox_records') }}
