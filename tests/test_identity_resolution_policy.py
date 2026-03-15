# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module tests the EpistemicIdentityResolutionPolicy.
"""

import uuid
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_liver_tox.utils.identity_resolution_policy import (
    EpistemicIdentityResolutionPolicy,
)


def test_apply_deterministic_identity_success() -> None:
    """Verifies that the policy successfully injects deterministic coreason_id."""
    records = [{"uid": "123", "data": "A"}, {"uid": "456", "data": "B"}]
    enriched_records = EpistemicIdentityResolutionPolicy.apply_deterministic_identity(records)

    assert len(enriched_records) == 2
    assert "coreason_id" in enriched_records[0]
    assert "coreason_id" in enriched_records[1]

    # Deterministic check
    expected_uuid_1 = str(uuid.uuid5(EpistemicIdentityResolutionPolicy.NAMESPACE_LIVERTOX, "123"))
    expected_uuid_2 = str(uuid.uuid5(EpistemicIdentityResolutionPolicy.NAMESPACE_LIVERTOX, "456"))

    assert enriched_records[0]["coreason_id"] == expected_uuid_1
    assert enriched_records[1]["coreason_id"] == expected_uuid_2


def test_apply_deterministic_identity_empty_records() -> None:
    """Verifies that an empty list returns an empty list without error."""
    records: list[dict[str, Any]] = []
    enriched_records = EpistemicIdentityResolutionPolicy.apply_deterministic_identity(records)
    assert enriched_records == []


def test_apply_deterministic_identity_missing_key() -> None:
    """Verifies that an exception is raised when the identity key is missing."""
    records = [{"uid": "123"}, {"other_key": "456"}]
    with pytest.raises(ValueError, match="Record missing mandatory identity key: uid"):
        EpistemicIdentityResolutionPolicy.apply_deterministic_identity(records)


def test_apply_deterministic_identity_custom_key() -> None:
    """Verifies that the policy works with a custom identity key."""
    records = [{"custom_id": "789", "data": "C"}]
    enriched_records = EpistemicIdentityResolutionPolicy.apply_deterministic_identity(records, id_key="custom_id")

    assert len(enriched_records) == 1
    assert "coreason_id" in enriched_records[0]

    expected_uuid = str(uuid.uuid5(EpistemicIdentityResolutionPolicy.NAMESPACE_LIVERTOX, "789"))
    assert enriched_records[0]["coreason_id"] == expected_uuid


@given(st.lists(st.fixed_dictionaries({"uid": st.text(min_size=1)}), min_size=1, max_size=10))  # type: ignore[misc]
def test_apply_deterministic_identity_property_based(records: list[dict[str, str]]) -> None:
    """Property-based test to ensure deterministic identity injection over random inputs."""
    enriched_records = EpistemicIdentityResolutionPolicy.apply_deterministic_identity(records)

    assert len(enriched_records) == len(records)
    for orig, enriched in zip(records, enriched_records, strict=False):
        assert enriched["uid"] == orig["uid"]
        assert "coreason_id" in enriched
        expected_uuid = str(uuid.uuid5(EpistemicIdentityResolutionPolicy.NAMESPACE_LIVERTOX, orig["uid"]))
        assert enriched["coreason_id"] == expected_uuid
