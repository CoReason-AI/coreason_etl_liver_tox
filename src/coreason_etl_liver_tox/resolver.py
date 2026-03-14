# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

"""Identity resolution logic generating deterministic IDs in Python before Postgres insertion."""

import uuid
from datetime import UTC, datetime
from typing import Any

import polars as pl

# Use the NCBI LiverTox Book ID as the deterministic namespace root
NAMESPACE_LIVERTOX = uuid.uuid5(uuid.NAMESPACE_OID, "NBK547852")


def _generate_uuid5_series(uid_series: pl.Series) -> pl.Series:
    """Vectorized application of UUID5 over a Polars string series.

    Args:
        uid_series: A Polars series containing NCBI UIDs.

    Returns:
        pl.Series: A new series containing the generated UUID5 strings.
    """
    return pl.Series([str(uuid.uuid5(NAMESPACE_LIVERTOX, str(uid))) if uid is not None else None for uid in uid_series])


def resolve_identities(records: list[dict[str, Any]]) -> pl.DataFrame:
    """Resolve and append deterministic identities to a batch of LiverTox records.

    AGENT INSTRUCTION: Uses Polars `map_batches` to safely vectorize Python's native `uuid5`
    over the ID column, strictly avoiding Postgres extension dependencies.

    Args:
        records: A list of cleaned Python dictionaries containing at least a "uid" key.

    Returns:
        pl.DataFrame: A Polars dataframe structured identically to `bronze_livertox_raw`.
    """
    if not records:
        return pl.DataFrame(
            schema={
                "coreason_id": pl.String,
                "uid": pl.String,
                "ingestion_ts": pl.Datetime(time_unit="us", time_zone="UTC"),
                "raw_data": pl.Object,
            }
        )

    # Convert the dicts into a Polars DataFrame where the dictionary itself is the raw_data
    df = pl.DataFrame({"raw_data": records})

    # Extract uid (assuming the dictionary has an explicit uid top-level, or buried in BookData)
    # The specification states: "uid: The NCBI Book Chapter ID." Usually this is at the root if
    # the client or parser puts it there. If not, we extract it from raw_data.

    df = df.with_columns(pl.col("raw_data").map_elements(lambda d: d.get("uid"), return_dtype=pl.String).alias("uid"))

    df = df.with_columns(
        coreason_id=pl.col("uid").map_batches(_generate_uuid5_series, return_dtype=pl.String),
        ingestion_ts=pl.lit(datetime.now(UTC)),
    )

    # Ensure exact column order as expected by Bronze layer
    return df.select(["coreason_id", "uid", "ingestion_ts", "raw_data"])
