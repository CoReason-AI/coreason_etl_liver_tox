# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError

from coreason_etl_liver_tox.client import LiverToxClient
from coreason_etl_liver_tox.config import LiverToxConfig


@pytest.fixture
def config() -> LiverToxConfig:
    return LiverToxConfig(ncbi_base_url="https://mock.ncbi.gov", api_key="secret-key", retmax=100)


@pytest.fixture
def client(config: LiverToxConfig) -> LiverToxClient:
    return LiverToxClient(config)


def test_get_base_params(client: LiverToxClient) -> None:
    """Verify that _get_base_params correctly injects the API key when available."""
    params = client._get_base_params()
    assert params["db"] == "books"
    assert params["api_key"] == "secret-key"


def test_get_base_params_no_key() -> None:
    """Verify that _get_base_params omits the API key when None."""
    config = LiverToxConfig(ncbi_base_url="https://mock.ncbi.gov", api_key=None, retmax=100)
    client_no_key = LiverToxClient(config)
    params = client_no_key._get_base_params()
    assert params["db"] == "books"
    assert "api_key" not in params


@patch("requests.Session.get")
def test_esearch_success(mock_get: MagicMock, client: LiverToxClient) -> None:
    """Verify that esearch successfully extracts count, webenv, and query_key."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "esearchresult": {
            "count": "1300",
            "webenv": "test_webenv_token",
            "querykey": "1",
        }
    }
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    result = client.esearch()

    assert result["count"] == 1300
    assert result["webenv"] == "test_webenv_token"
    assert result["query_key"] == "1"

    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert args[0] == "https://mock.ncbi.gov/esearch.fcgi"
    assert kwargs["params"]["term"] == "livertox[book]"


@patch("requests.Session.get")
def test_esearch_http_error(mock_get: MagicMock, client: LiverToxClient) -> None:
    """Verify that esearch raises an HTTPError on a failed response."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError("404 Client Error")
    mock_get.return_value = mock_response

    with pytest.raises(HTTPError, match="404 Client Error"):
        client.esearch()


@patch("requests.Session.get")
def test_esearch_invalid_json(mock_get: MagicMock, client: LiverToxClient) -> None:
    """Verify that esearch raises a KeyError when the JSON is malformed."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"invalid_structure": {}}
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    with pytest.raises(KeyError, match="Invalid E-Search response structure"):
        client.esearch()


@patch("requests.Session.get")
def test_efetch_batch_success(mock_get: MagicMock, client: LiverToxClient) -> None:
    """Verify that _efetch_batch retrieves the raw XML bytes correctly."""
    mock_response = MagicMock()
    mock_response.content = b"<BookData><Chapter>Test</Chapter></BookData>"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    # Force bypass of rate limit sleep to speed up test execution
    with patch("time.sleep", return_value=None):
        result = client._efetch_batch("test_env", "test_query", 0)

    assert result == b"<BookData><Chapter>Test</Chapter></BookData>"
    args, kwargs = mock_get.call_args
    assert args[0] == "https://mock.ncbi.gov/efetch.fcgi"
    assert kwargs["params"]["WebEnv"] == "test_env"
    assert kwargs["params"]["retstart"] == "0"
    assert kwargs["params"]["retmax"] == "100"


@patch("requests.Session.get")
def test_iter_efetch(mock_get: MagicMock, client: LiverToxClient) -> None:
    """Verify that iter_efetch accurately paginates through batches."""
    mock_response_1 = MagicMock()
    mock_response_1.content = b"<Batch>1</Batch>"

    mock_response_2 = MagicMock()
    mock_response_2.content = b"<Batch>2</Batch>"

    mock_get.side_effect = [mock_response_1, mock_response_2]

    # Total count 200, retmax is 100, so we expect exactly 2 iterations.
    with patch("time.sleep", return_value=None):
        batches = list(client.iter_efetch("env", "key", 200))

    assert len(batches) == 2
    assert batches[0] == b"<Batch>1</Batch>"
    assert batches[1] == b"<Batch>2</Batch>"

    # Check retstart offset on second call
    assert mock_get.call_count == 2
    _, kwargs = mock_get.call_args_list[1]
    assert kwargs["params"]["retstart"] == "100"
