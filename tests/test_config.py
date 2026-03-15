# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

import os
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from coreason_etl_liver_tox.config import LiverToxConfig


def test_default_config() -> None:
    """Verify that LiverToxConfig initializes with the correct default values."""
    config = LiverToxConfig()
    assert config.ncbi_base_url == "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    assert config.api_key is None
    assert config.retmax == 500


def test_custom_config() -> None:
    """Verify that LiverToxConfig initializes with provided values."""
    config = LiverToxConfig(ncbi_base_url="https://test.api", api_key="secret", retmax=100)
    assert config.ncbi_base_url == "https://test.api"
    assert config.api_key == "secret"
    assert config.retmax == 100


def test_retmax_exceeds_maximum() -> None:
    """Verify that an error is raised if retmax exceeds 10,000."""
    with pytest.raises(ValidationError):
        LiverToxConfig(retmax=10001)


def test_retmax_below_minimum() -> None:
    """Verify that an error is raised if retmax is less than or equal to 0."""
    with pytest.raises(ValidationError):
        LiverToxConfig(retmax=0)

    with pytest.raises(ValidationError):
        LiverToxConfig(retmax=-1)


@given(st.integers(min_value=1, max_value=10000))
def test_valid_retmax(retmax_val: int) -> None:
    """Verify that LiverToxConfig handles random valid retmax integers."""
    config = LiverToxConfig(retmax=retmax_val)
    assert config.retmax == retmax_val


def test_from_env_defaults() -> None:
    """Verify from_env loads defaults when no environment variables are present."""
    with patch.dict(os.environ, {}, clear=True):
        config = LiverToxConfig.from_env()
        assert config.ncbi_base_url == "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        assert config.api_key is None
        assert config.retmax == 500


def test_from_env_custom_values() -> None:
    """Verify from_env correctly parses valid environment variables."""
    env_vars = {
        "NCBI_BASE_URL": "https://custom.api",
        "NCBI_API_KEY": "test-key-123",
        "NCBI_RETMAX": "1234",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        config = LiverToxConfig.from_env()
        assert config.ncbi_base_url == "https://custom.api"
        assert config.api_key == "test-key-123"
        assert config.retmax == 1234


def test_from_env_invalid_retmax_type() -> None:
    """Verify from_env falls back to default if NCBI_RETMAX is not an integer."""
    env_vars = {
        "NCBI_RETMAX": "invalid_integer",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        config = LiverToxConfig.from_env()
        assert config.retmax == 500
