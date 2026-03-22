{{ config(
    materialized='view'
) }}

WITH source_data AS (
    SELECT
        coreason_id,
        uid AS ncbi_uid,
        COALESCE(
            raw_data ->> '@id',                                    -- Archive Root ID (e.g., "Brodalumab")
            raw_data #>> '{book-part-meta,title-group,title,0}',   -- Archive Nested Title
            raw_data #>> '{book-part-meta,title-group,title}',
            raw_data #>> '{article_title}',
            raw_data #>> '{ArticleTitle}',
            raw_data #>> '{title,0}',
            raw_data #>> '{title}',
            'Unknown Agent'
        ) AS agent_name,

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
