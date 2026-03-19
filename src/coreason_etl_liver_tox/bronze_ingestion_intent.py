# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module encapsulates the Bronze layer ingestion intent
for LiverTox monographs via `dlt`, strictly managing the history server
pagination, parsing, shift-left identity generation, and raw document extraction.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt

from coreason_etl_liver_tox.config.settings import settings
from coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy import EpistemicEUtilitiesClientPolicy
from coreason_etl_liver_tox.utils.identity_resolution_policy import EpistemicIdentityResolutionPolicy
from coreason_etl_liver_tox.utils.logger import logger
from coreason_etl_liver_tox.utils.xml_transmutation_policy import EpistemicXmlTransmutationPolicy


@dlt.source(max_table_nesting=0)
def livertox_source() -> Any:
    """
    AGENT INSTRUCTION: Defines the dlt source for LiverTox ingestion.
    Enforces `max_table_nesting=0` globally to prevent deep dict unpacking
    into fragmented relational tables.
    """
    return livertox_resource()


@dlt.resource(name="coreason_etl_liver_tox_bronze_livertox_raw", write_disposition="merge", primary_key="coreason_id")
def livertox_resource() -> Iterator[list[dict[str, Any]]]:
    """
    AGENT INSTRUCTION: The core dlt resource executing the History Server
    pagination and yielding batches of enriched, deterministically identified records.
    """
    client = EpistemicEUtilitiesClientPolicy()
    book_id = settings.livertox_book_id
    search_term = f"{book_id}[book]"

    logger.info(f"Initiating esearch for term: {search_term}")
    search_response = client.esearch_history_manifold(db="books", term=search_term)

    # Validate esearch response structure
    try:
        if "esearchresult" not in search_response:
            raise ValueError("Invalid esearch response format: Missing esearchresult")
        esearchresult = search_response["esearchresult"]
        count = int(esearchresult["count"])
        query_key = esearchresult["querykey"]
        webenv = search_response["esearchresult"]["webenv"]

    except KeyError as e:
        logger.exception(f"Missing expected keys in NCBI esearch response: {e}")
        raise ValueError(f"Invalid esearch response format: {e}") from e

    logger.info(f"Esearch completed. Total records found: {count}. query_key: {query_key}, WebEnv: {webenv}")

    if count == 0:  # pragma: no cover
        logger.info("No records found to fetch.")
        return  # pragma: no cover

    retmax = settings.ncbi_retmax
    retstart = 0

    while retstart < count:
        logger.info(f"Fetching batch from {retstart} to {min(retstart + retmax, count)}")
        xml_batch = client.efetch_xml_transmutation(
            db="books",
            query_key=query_key,
            webenv=webenv,
            retstart=retstart,
            retmax=retmax,
        )

        # Transmute XML to dict enforcing list tags
        parsed_dict = EpistemicXmlTransmutationPolicy.transmute_xml_to_dict(xml_batch)

        # Extract the list of BookDocument records from the NCBI payload.
        # The structure is typically: EntrezSystem->BookDocument (if multiple, it's a list)
        documents = _extract_book_documents(parsed_dict)

        if not documents:
            logger.warning(f"No documents extracted in batch starting at {retstart}. Breaking loop.")
            break

        processed_batch = []
        for doc in documents:
            try:
                # The NCBI UID is typically at the root of the document, or nested under article-id.
                # In NCBI Books, 'article-id' is often used, or 'PMID'. Let's find it.
                # A safer fallback is extracting 'id' or searching recursively if needed.
                # For LiverTox books, we typically need to ensure a unique key exists.

                # We need to guarantee a 'uid' field exists for identity resolution.
                doc_id = _extract_uid(doc)

                # Extract clinical text blocks (summary, score) and attach them directly
                # at the root level so dbt can easily query them out of JSONB.
                blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(doc)

                # Assemble the cleaned, flat-ish Bronze record
                cleaned_doc = {
                    "uid": doc_id,
                    "ingestion_ts": datetime.now(UTC).isoformat(),
                    "raw_data": doc,
                    "extracted_blocks": blocks,
                    "book_id": book_id,  # for context
                }
                processed_batch.append(cleaned_doc)
            except Exception as e:
                logger.error(f"Error processing a document in batch starting at {retstart}: {e}")
                continue

        # Apply deterministic identity resolution (shift-left)
        if processed_batch:
            enriched_batch = EpistemicIdentityResolutionPolicy.apply_deterministic_identity(
                processed_batch, id_key="uid"
            )
            yield enriched_batch

        retstart += retmax


def _extract_book_documents(parsed_dict: dict[str, Any]) -> list[dict[str, Any]]:
    """Helper to safely extract the list of documents from the varying NCBI XML roots."""
    docs = []

    # Check for 'BookDocumentSet'
    if "BookDocumentSet" in parsed_dict:
        bds = parsed_dict["BookDocumentSet"]
        if "BookDocument" in bds:
            bd = bds["BookDocument"]
            if isinstance(bd, list):
                docs.extend(bd)
            else:
                docs.append(bd)
    # Check for 'pmc-articleset' (common if retmode XML returns PMC style)
    elif "pmc-articleset" in parsed_dict:
        arts = parsed_dict["pmc-articleset"]
        if "article" in arts:
            art = arts["article"]
            if isinstance(art, list):
                docs.extend(art)
            else:
                docs.append(art)
    # Generic fallback: search for list-like lists or top-level items
    else:
        # Just return the top level values if we can't find a known root,
        # hoping it's a flat structure or the single document itself
        for val in parsed_dict.values():
            if isinstance(val, list):
                docs.extend(val)
            elif isinstance(val, dict):
                docs.append(val)

    return docs


def _extract_uid(document: dict[str, Any]) -> str:
    """Helper to extract a unique ID from an NCBI book document."""
    # Livertox articles typically have <article-id pub-id-type="pmid">...
    if "article-id" in document:
        a_ids = document["article-id"]
        if isinstance(a_ids, list):
            for a_id in a_ids:
                if isinstance(a_id, dict) and "#text" in a_id:
                    return str(a_id["#text"])
                if isinstance(a_id, str):
                    return a_id
        elif isinstance(a_ids, dict) and "#text" in a_ids:
            return str(a_ids["#text"])
        elif isinstance(a_ids, str):
            return a_ids

    # Fallback to book-id if present
    if "book-id" in document:
        b_ids = document["book-id"]
        if isinstance(b_ids, list):
            for b_id in b_ids:
                if isinstance(b_id, dict) and "#text" in b_id:
                    return str(b_id["#text"])
                if isinstance(b_id, str):
                    return b_id
        elif isinstance(b_ids, dict) and "#text" in b_ids:
            return str(b_ids["#text"])
        elif isinstance(b_ids, str):
            return b_ids

    # Try generic 'id'
    if "id" in document:
        return str(document["id"])

    # If all else fails, serialize the dict and hash it to ensure uniqueness
    import hashlib
    import json

    doc_str = json.dumps(document, sort_keys=True)
    return hashlib.sha256(doc_str.encode()).hexdigest()
