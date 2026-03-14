# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day license requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

"""dlt pipeline construction integrating XML parsing and Polars identities."""

from collections.abc import Iterator
from typing import Any

import dlt
import polars as pl

from coreason_etl_liver_tox.client import LiverToxClient
from coreason_etl_liver_tox.config import LiverToxConfig
from coreason_etl_liver_tox.parser import (
    isolate_hepatotoxicity_summary,
    isolate_likelihood_score,
    parse_livertox_xml,
)
from coreason_etl_liver_tox.resolver import resolve_identities
from coreason_etl_liver_tox.utils.logger import logger


@dlt.resource(name="bronze_livertox_raw", write_disposition="merge", primary_key="coreason_id", max_table_nesting=0)
def livertox_records(config: LiverToxConfig) -> Iterator[pl.DataFrame]:
    """Fetch LiverTox XML, parse, isolate strings, resolve Polars IDs, and yield to dlt.

    AGENT INSTRUCTION: max_table_nesting=0 forces all nested structures to JSONB.
    This guarantees zero crash-outs from schema inference.

    Args:
        config: The initialized LiverToxConfig.

    Yields:
        pl.DataFrame: The formatted bronze_livertox_raw data chunk.
    """
    client = LiverToxClient(config)

    try:
        search_data = client.esearch()
        webenv = search_data["webenv"]
        query_key = search_data["query_key"]
        total_count = search_data["count"]
    except Exception:
        logger.exception("E-Search initialization failed")
        raise

    logger.info("Began ingestion phase for LiverTox", total_records=total_count)

    for xml_batch in client.iter_efetch(webenv, query_key, total_count):
        try:
            parsed_batch = parse_livertox_xml(xml_batch)

            # The root element of an efetch from books is typically an outer wrapper
            # BookDocumentSet -> BookDocument. If it's a single element, xmltodict
            # might not make it a list unless we forced it (but we didn't force BookDocument).
            # Let's cleanly extract BookDocument records if they exist.
            # E-utilities efetch returns <BookDocumentSet><BookDocument>...
            book_docs = parsed_batch.get("BookDocumentSet", {}).get("BookDocument", [])

            if not isinstance(book_docs, list):
                book_docs = [book_docs]  # pragma: no cover

            clean_records = []
            for doc in book_docs:
                if not doc:
                    continue

                # UIDs in NCBI are sometimes found at doc.get("BookData", {}).get("@id")
                # or doc.get("PMID", {}).get("#text")
                # The FRD expects a 'uid' to be isolated.
                uid = None
                if "PMID" in doc:
                    uid = (
                        str(doc["PMID"].get("#text", "")) if isinstance(doc["PMID"], dict) else str(doc["PMID"])
                    )  # pragma: no cover
                elif "BookData" in doc and "@id" in doc["BookData"]:
                    uid = doc["BookData"]["@id"]
                elif "ArticleIdList" in doc:  # pragma: no cover
                    # Sometimes nested here
                    art_ids = doc["ArticleIdList"].get("ArticleId", [])
                    if not isinstance(art_ids, list):
                        art_ids = [art_ids]
                    for art_id in art_ids:
                        if isinstance(art_id, dict) and art_id.get("@IdType") == "pmid":
                            uid = art_id.get("#text")
                            break

                # Safely push into the nested structure to execute isolations if required early
                # The specifications say "Before yielding to dlt, the Python script must traverse
                # the parsed dictionary to isolate the core text blocks".
                doc["_hepatotoxicity_summary"] = isolate_hepatotoxicity_summary(doc)
                doc["_likelihood_score"] = isolate_likelihood_score(doc)

                clean_records.append({"uid": uid, "data": doc})

            df = resolve_identities(clean_records)
            yield df

        except Exception as e:
            logger.error("Failed to process batch", error=str(e))
            raise


@dlt.source(name="livertox_source", max_table_nesting=0)
def livertox_source(config: LiverToxConfig) -> Iterator[Any]:
    """Construct the dlt source for LiverTox ingestion.

    Args:
        config: The initialized configuration.

    Returns:
        Iterator[Any]: The bound resource object.
    """
    yield livertox_records(config)


def create_pipeline(destination: str = "postgres") -> dlt.Pipeline:
    """Create the executable dlt pipeline.

    Args:
        destination: Target destination standard (default: postgres).

    Returns:
        dlt.Pipeline: Configured dlt pipeline.
    """
    return dlt.pipeline(
        pipeline_name="coreason_etl_liver_tox",
        destination=destination,
        dataset_name="bronze",
        progress="log",
    )
