-- Copyright (c) 2026 CoReason Inc.
-- This software is proprietary and dual-licensed.
-- Licensed under the Prosperity Public License 3.0 (the "License").
-- Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

{{ config(
    materialized='table',
    description="AGENT INSTRUCTION: Parsed and cleaned LiverTox clinical records containing extracted scores."
) }}

WITH raw_bronze AS (
    SELECT
        coreason_id,
        uid AS ncbi_uid,
        -- Extract JSONB fields from the _extracted dict we populated in Python
        raw_data->'_extracted'->>'agent_name' AS agent_name,
        raw_data->'_extracted'->>'hepatotoxicity_summary' AS hepatotoxicity_summary,
        raw_data->'_extracted'->>'likelihood_score_block' AS likelihood_score_block,
        raw_data->'_extracted'->>'mechanism_of_injury' AS mechanism_of_injury
    FROM {{ source('livertox_source', 'bronze_livertox_raw') }}
),

extracted AS (
    SELECT
        coreason_id,
        ncbi_uid,
        agent_name,
        -- Use specific regex requirement to isolate the DILI likelihood score from the text block
        SUBSTRING(likelihood_score_block FROM 'Likelihood score:\s*([A-EX](?:\[[^\]]+\]|\*)?)') AS dili_likelihood_score,
        hepatotoxicity_summary,
        mechanism_of_injury
    FROM raw_bronze
)

SELECT
    coreason_id,
    ncbi_uid,
    agent_name,
    dili_likelihood_score,
    hepatotoxicity_summary,
    mechanism_of_injury
FROM extracted
