{{ config(materialized='table') }}

WITH raw AS (
    SELECT
        coreason_id,
        uid AS ncbi_uid,
        ingestion_ts,
        raw_data,
        -- Access the nested 'data' block built during python dlt yield
        raw_data->'data' AS nested_data
    FROM {{ source('public', 'bronze_livertox_raw') }}
),

extracted AS (
    SELECT
        coreason_id,
        ncbi_uid,
        ingestion_ts,
        -- Extract the agent name from BookData -> Chapter -> title
        -- Based on standard NCBI LiverTox XML structure, the chapter title is the drug name
        nested_data->'BookData'->'Chapter'->'title'->0->>'#text' AS agent_name,

        -- Extract directly from Python-isolated fields
        nested_data->>'_hepatotoxicity_summary' AS hepatotoxicity_summary,
        nested_data->>'_likelihood_score' AS isolated_likelihood_text

    FROM raw
)

SELECT
    coreason_id,
    ncbi_uid,
    ingestion_ts,
    agent_name,
    hepatotoxicity_summary,

    -- Mandatory Regex Extraction Rule for 'dili_likelihood_score'
    SUBSTRING(isolated_likelihood_text FROM 'Likelihood score:\s*([A-EX](?:\[[^\]]+\]|\*)?)') AS dili_likelihood_score

FROM extracted
