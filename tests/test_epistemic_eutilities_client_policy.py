# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module tests the EpistemicEUtilitiesClientPolicy.
"""

from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests

from coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy import (
    EpistemicEUtilitiesClientPolicy,
)


@pytest.fixture
def mock_settings() -> Any:
    """Mocks the application configuration settings."""
    with patch("coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.settings") as mock:
        mock.ncbi_eutils_base_url = "https://mock.ncbi.gov/entrez/eutils"
        mock.ncbi_api_key = "test_api_key"
        yield mock


@pytest.fixture
def mock_settings_no_api_key() -> Any:
    """Mocks the application configuration settings without an API key."""
    with patch("coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.settings") as mock:
        mock.ncbi_eutils_base_url = "https://mock.ncbi.gov/entrez/eutils"
        mock.ncbi_api_key = None
        yield mock


@pytest.fixture
def client_policy() -> EpistemicEUtilitiesClientPolicy:
    """Returns an instance of EpistemicEUtilitiesClientPolicy."""
    return EpistemicEUtilitiesClientPolicy()


def test_esearch_history_manifold_success(mock_settings: Any, client_policy: EpistemicEUtilitiesClientPolicy) -> None:
    """Verifies that an ESearch request is successfully processed."""
    _ = mock_settings
    mock_response = Mock()
    mock_response.json.return_value = {"esearchresult": {"querykey": "1", "webenv": "test_webenv"}}
    mock_response.raise_for_status.return_value = None

    with patch.object(client_policy.session, "get", return_value=mock_response) as mock_get:
        result = client_policy.esearch_history_manifold("books", "livertox[book]")

        mock_get.assert_called_once_with(
            "https://mock.ncbi.gov/entrez/eutils/esearch.fcgi",
            params={
                "db": "books",
                "term": "livertox[book]",
                "usehistory": "y",
                "retmode": "json",
                "api_key": "test_api_key",
            },
            timeout=30,
        )
        assert result == {"esearchresult": {"querykey": "1", "webenv": "test_webenv"}}


def test_efetch_xml_transmutation_success(mock_settings: Any, client_policy: EpistemicEUtilitiesClientPolicy) -> None:
    """Verifies that an EFetch request is successfully processed."""
    _ = mock_settings
    mock_response = Mock()
    mock_response.text = "<xml>test</xml>"
    mock_response.raise_for_status.return_value = None

    with patch.object(client_policy.session, "get", return_value=mock_response) as mock_get:
        result = client_policy.efetch_xml_transmutation("books", "1", "test_webenv", 0, 100)

        mock_get.assert_called_once_with(
            "https://mock.ncbi.gov/entrez/eutils/efetch.fcgi",
            params={
                "db": "books",
                "query_key": "1",
                "WebEnv": "test_webenv",
                "retstart": 0,
                "retmax": 100,
                "retmode": "xml",
                "api_key": "test_api_key",
            },
            timeout=30,
        )
        assert result == "<xml>test</xml>"


def test_execute_request_error(mock_settings: Any, client_policy: EpistemicEUtilitiesClientPolicy) -> None:
    """Verifies that an HTTP error raises an exception."""
    _ = mock_settings
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")

    with (
        patch.object(client_policy.session, "get", return_value=mock_response),
        pytest.raises(requests.exceptions.HTTPError),
    ):
        client_policy._execute_request("esearch.fcgi", {})


def test_execute_request_no_api_key(
    mock_settings_no_api_key: Any, client_policy: EpistemicEUtilitiesClientPolicy
) -> None:
    """Verifies that a request without an API key does not include the api_key param."""
    _ = mock_settings_no_api_key
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None

    with patch.object(client_policy.session, "get", return_value=mock_response) as mock_get:
        client_policy._execute_request("esearch.fcgi", {})

        mock_get.assert_called_once_with(
            "https://mock.ncbi.gov/entrez/eutils/esearch.fcgi",
            params={},
            timeout=30,
        )
