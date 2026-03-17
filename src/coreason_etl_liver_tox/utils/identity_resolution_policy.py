# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module provides the Identity Resolution policy using Polars
to deterministically generate UUID5 identifiers.
"""

import uuid
from typing import Any

import polars as pl

from coreason_etl_liver_tox.utils.logger import logger


class EpistemicIdentityResolutionPolicy:
    """
    AGENT INSTRUCTION: Encapsulates the execution policy for generating deterministic
    coreason_id (UUID5) using Polars vectorized map_batches.
    """

    NAMESPACE_LIVERTOX = uuid.UUID("a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d")

    @classmethod
    def apply_deterministic_identity(cls, records: list[dict[str, Any]], id_key: str = "uid") -> list[dict[str, Any]]:
        """
        Generates a deterministic 'coreason_id' for each record based on the specified id_key.
        Returns a new list of dictionaries with the 'coreason_id' injected.
        """
        if not records:
            logger.debug("Received empty records list for identity resolution.")
            return []

        logger.debug(f"Applying deterministic identity resolution using key: {id_key}")

        # Extract the IDs to be vectorized
        try:
            uids = [str(record[id_key]) for record in records]
        except KeyError as e:
            logger.exception(f"Missing mandatory identity key '{id_key}' in records.")
            raise ValueError(f"Record missing mandatory identity key: {id_key}") from e

        # Create a Polars DataFrame for the UIDs
        df = pl.DataFrame({"uid": uids}, schema={"uid": pl.Utf8})

        # Vectorized UUID5 generation using map_batches on the expression
        def _generate_uuid5(s: pl.Series) -> pl.Series:
            return pl.Series([str(uuid.uuid5(cls.NAMESPACE_LIVERTOX, val)) for val in s], dtype=pl.Utf8)

        # Apply map_batches to generate the new column
        df = df.with_columns(pl.col("uid").map_batches(_generate_uuid5, return_dtype=pl.Utf8).alias("coreason_id"))

        coreason_ids_series = df.get_column("coreason_id")

        # Inject the new IDs back into the dictionaries
        enriched_records = []
        for record, coreason_id in zip(records, coreason_ids_series, strict=False):
            # Create a shallow copy to avoid mutating the original input unexpectedly
            new_record = record.copy()
            new_record["coreason_id"] = coreason_id
            enriched_records.append(new_record)

        return enriched_records
