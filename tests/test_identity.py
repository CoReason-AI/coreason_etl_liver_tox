# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

from typing import Any

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_liver_tox.identity import EpistemicIdentityCascadeTask


def test_resolve_identities_valid() -> None:
    """AGENT INSTRUCTION: Ensure that a valid batch of records is properly resolved with deterministic UUIDs."""
    records = [
        {"uid": "1", "name": "A"},
        {"uid": "2", "name": "B"},
    ]

    resolved = EpistemicIdentityCascadeTask.resolve_identities(records)

    assert len(resolved) == 2

    assert "coreason_id" in resolved[0]
    assert resolved[0]["uid"] == "1"
    assert "ingestion_ts" in resolved[0]
    assert "raw_data" in resolved[0]
    assert resolved[0]["raw_data"]["name"] == "A"

    assert "coreason_id" in resolved[1]
    assert resolved[1]["uid"] == "2"


def test_resolve_identities_missing_uid() -> None:
    """AGENT INSTRUCTION: Ensure that records missing a 'uid' are gracefully filtered out."""
    records: list[dict[str, Any]] = [
        {"uid": "1", "name": "A"},
        {"name": "B"},  # Missing UID
        {"uid": None, "name": "C"},  # None UID
    ]

    resolved = EpistemicIdentityCascadeTask.resolve_identities(records)

    assert len(resolved) == 1
    assert resolved[0]["uid"] == "1"


def test_resolve_identities_empty() -> None:
    """AGENT INSTRUCTION: Ensure that empty lists return empty lists."""
    assert EpistemicIdentityCascadeTask.resolve_identities([]) == []


def test_resolve_identities_all_invalid() -> None:
    """AGENT INSTRUCTION: Ensure that lists with no valid UIDs return empty lists."""
    assert EpistemicIdentityCascadeTask.resolve_identities([{"name": "A"}]) == []


@given(uid1=st.text(min_size=1))
def test_resolve_identities_deterministic(uid1: str) -> None:
    """AGENT INSTRUCTION: Property-based edge cases to guarantee hash determinism."""
    # Ensure same inputs yield same UUIDs
    records1 = [{"uid": uid1}]
    resolved1 = EpistemicIdentityCascadeTask.resolve_identities(records1)

    records2 = [{"uid": uid1}]
    resolved2 = EpistemicIdentityCascadeTask.resolve_identities(records2)

    assert resolved1[0]["coreason_id"] == resolved2[0]["coreason_id"]


def test_resolve_identities_polars_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """AGENT INSTRUCTION: Ensure that an error from Polars correctly raises a RuntimeError."""

    def mock_map_batches(*_args: Any, **_kwargs: Any) -> Any:
        raise Exception("Polars Boom!")

    monkeypatch.setattr(pl.Expr, "map_batches", mock_map_batches)

    with pytest.raises(RuntimeError) as exc_info:
        EpistemicIdentityCascadeTask.resolve_identities([{"uid": "1"}])
    assert "Polars identity resolution failed" in str(exc_info.value)
