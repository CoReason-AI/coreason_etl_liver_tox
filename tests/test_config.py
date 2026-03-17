# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from coreason_etl_liver_tox.config import FederatedIngestionConfigManifest


def test_config_manifest_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    AGENT INSTRUCTION: Ensure that valid environment variables are successfully
    parsed into the configuration manifest.
    """
    monkeypatch.setenv("LIVERTOX_API_KEY", "test_key_123")
    monkeypatch.setenv("LIVERTOX_RETMAX", "100")

    config = FederatedIngestionConfigManifest()

    assert config.api_key == "test_key_123"
    assert config.retmax == 100
    assert config.base_url == "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"


def test_config_manifest_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    AGENT INSTRUCTION: Ensure validation fails when the required `api_key`
    is missing from the environment.
    """
    monkeypatch.delenv("LIVERTOX_API_KEY", raising=False)

    with pytest.raises(ValidationError) as exc_info:
        FederatedIngestionConfigManifest()

    assert "api_key" in str(exc_info.value)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    api_key=st.text(min_size=1, alphabet=st.characters(blacklist_categories=("Cc", "Cs"))),
    retmax=st.integers(min_value=1, max_value=10000),
)
def test_config_manifest_property_based(monkeypatch: pytest.MonkeyPatch, api_key: str, retmax: int) -> None:
    """
    AGENT INSTRUCTION: Hypothesis-based property test for valid ranges of
    `api_key` and `retmax`.
    """
    monkeypatch.setenv("LIVERTOX_API_KEY", api_key)
    monkeypatch.setenv("LIVERTOX_RETMAX", str(retmax))

    config = FederatedIngestionConfigManifest()

    assert config.api_key == api_key
    assert config.retmax == retmax


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(retmax=st.one_of(st.integers(max_value=0), st.integers(min_value=10001)))
def test_config_manifest_invalid_retmax(monkeypatch: pytest.MonkeyPatch, retmax: int) -> None:
    """
    AGENT INSTRUCTION: Hypothesis-based property test for invalid ranges of
    `retmax` ensuring validation errors are triggered.
    """
    monkeypatch.setenv("LIVERTOX_API_KEY", "valid_key")
    monkeypatch.setenv("LIVERTOX_RETMAX", str(retmax))

    with pytest.raises(ValidationError):
        FederatedIngestionConfigManifest()
