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
import responses

from coreason_etl_liver_tox.client import EutilitiesAPIError, FederatedEutilitiesTask
from coreason_etl_liver_tox.config import FederatedIngestionConfigManifest


@pytest.fixture
def test_config(monkeypatch: pytest.MonkeyPatch) -> FederatedIngestionConfigManifest:
    """Fixture providing a mock configuration manifest."""
    monkeypatch.setenv("LIVERTOX_API_KEY", "mock_api_key")
    monkeypatch.setenv("LIVERTOX_RETMAX", "100")
    monkeypatch.setenv("LIVERTOX_BASE_URL", "https://mock.ncbi.nlm.nih.gov/entrez/eutils/")
    return FederatedIngestionConfigManifest()


@pytest.fixture
def eutils_client(test_config: FederatedIngestionConfigManifest) -> FederatedEutilitiesTask:
    """Fixture providing a configured FederatedEutilitiesTask."""
    return FederatedEutilitiesTask(config=test_config)


@responses.activate
def test_esearch_success(eutils_client: FederatedEutilitiesTask) -> None:
    """AGENT INSTRUCTION: Ensure that a successful E-Search request properly parses the response."""
    responses.add(
        responses.GET,
        "https://mock.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        json={"esearchresult": {"count": "1500", "querykey": "1", "webenv": "MCID_1234567890"}},
        status=200,
    )

    result = eutils_client.execute_esearch()
    assert result["count"] == 1500
    assert result["query_key"] == "1"
    assert result["webenv"] == "MCID_1234567890"


@responses.activate
def test_esearch_missing_fields(eutils_client: FederatedEutilitiesTask) -> None:
    """AGENT INSTRUCTION: Ensure that a successful E-Search request but missing required fields throws an error."""
    responses.add(
        responses.GET,
        "https://mock.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        json={
            "esearchresult": {
                "count": "1500",
                "querykey": "1",
                # Missing webenv
            }
        },
        status=200,
    )

    with pytest.raises(EutilitiesAPIError) as exc_info:
        eutils_client.execute_esearch()
    assert "missing 'querykey' or 'webenv'" in str(exc_info.value)


@responses.activate
def test_esearch_http_error(eutils_client: FederatedEutilitiesTask) -> None:
    """AGENT INSTRUCTION: Ensure that a failed HTTP request correctly raises EutilitiesAPIError."""
    responses.add(
        responses.GET,
        "https://mock.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        status=500,
    )

    with pytest.raises(EutilitiesAPIError) as exc_info:
        eutils_client.execute_esearch()
    assert "NCBI E-utilities API request failed" in str(exc_info.value)


@responses.activate
def test_efetch_pagination_success(eutils_client: FederatedEutilitiesTask) -> None:
    """AGENT INSTRUCTION: Ensure that E-Fetch pagination correctly iterates over total records and yields XML."""
    count = 250
    eutils_client.config.retmax = 100
    query_key = "1"
    webenv = "MCID_123"

    responses.add(
        responses.GET,
        "https://mock.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        body="<xml>batch1</xml>",
        status=200,
    )
    responses.add(
        responses.GET,
        "https://mock.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        body="<xml>batch2</xml>",
        status=200,
    )
    responses.add(
        responses.GET,
        "https://mock.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        body="<xml>batch3</xml>",
        status=200,
    )

    results = list(eutils_client.execute_efetch_pagination(count, query_key, webenv))

    assert len(results) == 3
    assert results[0] == "<xml>batch1</xml>"
    assert results[1] == "<xml>batch2</xml>"
    assert results[2] == "<xml>batch3</xml>"

    # Assert proper pagination arguments
    assert len(responses.calls) == 3
    assert "retstart=0" in responses.calls[0].request.url
    assert "retstart=100" in responses.calls[1].request.url
    assert "retstart=200" in responses.calls[2].request.url
    assert "retmax=100" in responses.calls[0].request.url
