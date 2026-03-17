# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

import datetime
import uuid
from typing import Any

import polars as pl

from coreason_etl_liver_tox.utils.logger import logger


class EpistemicIdentityCascadeTask:
    """
    AGENT INSTRUCTION: This class applies Shift-Left Identity Resolution.
    It takes semi-structured dictionaries, filters out those without UIDs,
    and applies a deterministic UUID5 hash across the batch using Polars,
    preventing the need for 'uuid-ossp' Postgres extensions downstream.
    """

    NAMESPACE_LIVERTOX = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # standard uuid.NAMESPACE_DNS used as base

    @classmethod
    def resolve_identities(cls, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        AGENT INSTRUCTION: Compute deterministic `coreason_id` and inject `ingestion_ts` into the payload.
        Transforms dictionaries into Polars DataFrames for vectorized UUID generation,
        and yields Bronze-ready payloads.
        """
        if not records:
            return []

        # Filter out records without a 'uid'
        valid_records = [r for r in records if r.get("uid") is not None]

        if not valid_records:
            logger.warning("No valid records with 'uid' found in batch.")
            return []

        # Use Polars to shift-left identity resolution
        df = pl.DataFrame(valid_records)
        ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()

        try:
            # Map UUID5 across the UID series
            df = df.with_columns(
                pl.col("uid")
                .map_batches(lambda s: pl.Series([str(uuid.uuid5(cls.NAMESPACE_LIVERTOX, str(val))) for val in s]))
                .alias("coreason_id"),
                pl.lit(ingestion_ts).alias("ingestion_ts"),
            )
        except Exception as e:
            logger.error(f"Polars identity resolution failed: {e}")
            raise RuntimeError(f"Polars identity resolution failed: {e}") from e

        # Restructure for dlt Bronze ingestion (max_table_nesting=0 schema pattern)
        return [
            {
                "coreason_id": row["coreason_id"],
                "uid": row["uid"],
                "ingestion_ts": row["ingestion_ts"],
                "raw_data": row,  # Entire dictionary lands in JSONB
            }
            for row in df.iter_rows(named=True)
        ]
