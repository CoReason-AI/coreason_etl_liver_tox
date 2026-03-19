{{ config(
    materialized='view'
) }}

WITH source_data AS (
    SELECT
        coreason_id,
        uid AS ncbi_uid,
        -- Extract from raw_data -> 'title' if present.
        -- Assuming JSONB path for title based on typical NCBI BookDocument structure:
        COALESCE(
            raw_data #>> '{BookDocument,ArticleTitle}',
            raw_data #>> '{article,front,article-meta,title-group,article-title}',
            raw_data #>> '{BookDocument,Book,BookTitle}',
            'Unknown Agent'
        ) AS agent_name,

        -- Extract clinical text blocks from extracted_blocks
        extracted_blocks ->> 'hepatotoxicity_summary' AS hepatotoxicity_summary,
        extracted_blocks ->> 'likelihood_score' AS isolated_text,
        extracted_blocks ->> 'mechanism_of_injury' AS mechanism_of_injury

    FROM {{ source('bronze', 'coreason_etl_liver_tox_bronze_livertox_raw') }}
)

SELECT
    coreason_id,
    ncbi_uid,
    agent_name,
    SUBSTRING(isolated_text FROM 'Likelihood score:\s*([A-EX](?:\[[^\]]+\]|\*)?)') AS dili_likelihood_score,
    hepatotoxicity_summary,
    mechanism_of_injury
FROM source_data
