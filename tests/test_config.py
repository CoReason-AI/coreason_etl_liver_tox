# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.


import pytest
from pydantic import ValidationError

from coreason_etl_liver_tox.config.settings import ConfigurationSettings, settings


def test_default_settings_load() -> None:
    """Test that default settings are correctly loaded."""
    assert settings.ncbi_eutils_base_url == "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    assert settings.ncbi_api_key is None
    assert settings.ncbi_retmax == 100
    assert settings.livertox_book_id == "NBK547852"
    assert settings.log_level == "INFO"


def test_custom_settings_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test loading settings from environment variables."""
    monkeypatch.setenv("NCBI_API_KEY", "test_key")
    monkeypatch.setenv("NCBI_RETMAX", "50")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    custom_settings = ConfigurationSettings()
    assert custom_settings.ncbi_api_key == "test_key"
    assert custom_settings.ncbi_retmax == 50
    assert custom_settings.log_level == "DEBUG"


def test_invalid_retmax_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that setting an invalid retmax raises a ValidationError."""
    monkeypatch.setenv("NCBI_RETMAX", "1000")  # Should be <= 500

    with pytest.raises(ValidationError):
        ConfigurationSettings()

    monkeypatch.setenv("NCBI_RETMAX", "0")  # Should be >= 1
    with pytest.raises(ValidationError):
        ConfigurationSettings()
