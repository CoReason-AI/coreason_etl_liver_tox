# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

import uuid

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_liver_tox.resolver import NAMESPACE_LIVERTOX, resolve_identities


def test_resolve_identities_empty_list() -> None:
    """Verify that an empty list returns an empty DataFrame with the correct schema."""
    df = resolve_identities([])
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0
    assert df.columns == ["coreason_id", "uid", "ingestion_ts", "raw_data"]
    assert df.schema["coreason_id"] == pl.String
    assert df.schema["uid"] == pl.String
    assert df.schema["raw_data"] == pl.Object


def test_resolve_identities_generates_correct_uuid() -> None:
    """Verify that coreason_id is generated deterministically using uuid5 and NAMESPACE_LIVERTOX."""
    records = [{"uid": "12345", "other_data": "test"}]
    df = resolve_identities(records)

    assert len(df) == 1
    assert df.columns == ["coreason_id", "uid", "ingestion_ts", "raw_data"]

    expected_uuid = str(uuid.uuid5(NAMESPACE_LIVERTOX, "12345"))
    assert df["coreason_id"][0] == expected_uuid
    assert df["uid"][0] == "12345"
    assert df["raw_data"][0] == records[0]


def test_resolve_identities_handles_missing_uid() -> None:
    """Verify behavior when uid is missing from the record."""
    records = [{"other_data": "test"}]
    df = resolve_identities(records)

    assert len(df) == 1
    assert df["uid"][0] is None
    assert df["coreason_id"][0] is None


@given(st.lists(st.integers(min_value=1, max_value=1000000).map(str), min_size=5, max_size=50))
def test_resolve_identities_determinism(uids: list[str]) -> None:
    """Verify deterministic generation over multiple batches and random uids."""
    records = [{"uid": uid} for uid in uids]
    df1 = resolve_identities(records)
    df2 = resolve_identities(records)

    # Convert to lists for easier comparison
    ids1 = df1["coreason_id"].to_list()
    ids2 = df2["coreason_id"].to_list()

    # Determinism: same input produces same output
    assert ids1 == ids2

    # All non-null IDs should be valid UUIDs
    for id_val in ids1:
        assert isinstance(id_val, str)
        assert len(id_val) == 36  # standard uuid length
        # Check it matches direct computation
        idx = ids1.index(id_val)
        assert id_val == str(uuid.uuid5(NAMESPACE_LIVERTOX, uids[idx]))
