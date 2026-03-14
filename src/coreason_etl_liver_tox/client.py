# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

"""NCBI E-utilities REST API client for LiverTox."""

from collections.abc import Iterator
from typing import Any

import requests
from ratelimit import limits, sleep_and_retry

from coreason_etl_liver_tox.config import LiverToxConfig
from coreason_etl_liver_tox.utils.logger import logger


class LiverToxClient:
    """Client for interacting with the NCBI E-utilities History Server.

    AGENT INSTRUCTION: Ensure that rate limits strictly adhere to the NCBI 10 requests/second policy
    when an API key is present. Paginate E-Fetch using the WebEnv token.
    """

    def __init__(self, config: LiverToxConfig) -> None:
        """Initialize the client with the provided configuration.

        Args:
            config: A validated LiverToxConfig instance.
        """
        self.config = config
        self.session = requests.Session()

    def _get_base_params(self) -> dict[str, str]:
        """Construct the base query parameters required for all NCBI requests.

        Returns:
            Dict[str, str]: The base dictionary of query parameters.
        """
        params = {"db": "books"}
        if self.config.api_key:
            params["api_key"] = self.config.api_key
        return params

    def esearch(self) -> dict[str, Any]:
        """Execute an E-Search query against the NCBI Books database for LiverTox.

        Posts the search to the History Server to retrieve the `WebEnv` and `query_key`.

        Returns:
            Dict[str, Any]: A dictionary containing `count`, `webenv`, and `query_key`.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request fails.
            KeyError: If the expected keys are missing from the NCBI response.
        """
        url = f"{self.config.ncbi_base_url}/esearch.fcgi"
        params = self._get_base_params()
        params.update(
            {
                "term": "livertox[book]",
                "usehistory": "y",
                "retmode": "json",
            }
        )

        logger.info("Executing E-Search on NCBI Books database", url=url)
        response = self.session.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        try:
            esearch_result = data["esearchresult"]
            return {
                "count": int(esearch_result["count"]),
                "webenv": esearch_result["webenv"],
                "query_key": esearch_result["querykey"],
            }
        except (KeyError, ValueError) as e:
            logger.error("Failed to parse E-Search response", error=str(e), response=data)
            raise KeyError(f"Invalid E-Search response structure: {e}") from e

    @sleep_and_retry  # type: ignore[misc, untyped-decorator]
    @limits(calls=10, period=1)  # type: ignore[misc, untyped-decorator]
    def _efetch_batch(self, webenv: str, query_key: str, retstart: int) -> bytes:
        """Fetch a single paginated batch of XML records from the History Server.

        This method is strictly rate-limited to 10 requests per second.

        Args:
            webenv: The WebEnv token from E-Search.
            query_key: The query_key from E-Search.
            retstart: The zero-based offset for pagination.

        Returns:
            bytes: The raw XML response body.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request fails.
        """
        url = f"{self.config.ncbi_base_url}/efetch.fcgi"
        params = self._get_base_params()
        params.update(
            {
                "WebEnv": webenv,
                "query_key": query_key,
                "retstart": str(retstart),
                "retmax": str(self.config.retmax),
                "retmode": "xml",
            }
        )

        logger.info("Executing E-Fetch batch", retstart=retstart, retmax=self.config.retmax)
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response.content

    def iter_efetch(self, webenv: str, query_key: str, total_count: int) -> Iterator[bytes]:
        """Generate batches of XML content by paginating through the total count.

        Args:
            webenv: The WebEnv token from E-Search.
            query_key: The query_key from E-Search.
            total_count: The total number of records available to fetch.

        Yields:
            bytes: The raw XML batch.
        """
        retstart = 0
        while retstart < total_count:
            xml_batch = self._efetch_batch(webenv, query_key, retstart)
            yield xml_batch
            retstart += self.config.retmax
