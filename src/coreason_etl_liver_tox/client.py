# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

from collections.abc import Generator
from typing import Any

import requests
from ratelimit import limits, sleep_and_retry

from coreason_etl_liver_tox.config import FederatedIngestionConfigManifest
from coreason_etl_liver_tox.utils.logger import logger


class EutilitiesAPIError(Exception):
    """AGENT INSTRUCTION: Exception raised for HTTP or API-level errors during E-utilities federation."""


class FederatedEutilitiesTask:
    """
    AGENT INSTRUCTION: This class encapsulates the NCBI E-utilities History Server pattern.
    It manages the HTTP requests, enforces rate limits, and orchestrates the paginated
    extraction of LiverTox data.
    """

    def __init__(self, config: FederatedIngestionConfigManifest) -> None:
        """
        Initialize the federated task with a deterministic configuration manifest.
        """
        self.config = config
        self.session = requests.Session()

    @sleep_and_retry
    @limits(calls=10, period=1)
    def _execute_request(self, method: str, url: str, params: dict[str, Any]) -> requests.Response:
        """
        AGENT INSTRUCTION: Execute an HTTP request against the NCBI API, strictly adhering
        to the Token Bucket rate limit of 10 requests per second.
        """
        # Inject API key into all requests
        params["api_key"] = self.config.api_key

        try:
            response = self.session.request(method=method, url=url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Request failed: {e}")
            raise EutilitiesAPIError(f"NCBI E-utilities API request failed: {e}") from e

    def execute_esearch(self) -> dict[str, Any]:
        """
        AGENT INSTRUCTION: Execute the E-Search capability. Posts the query to the
        History Server and retrieves the necessary environment tokens (`WebEnv` and `query_key`)
        along with the total count.
        """
        url = f"{self.config.base_url.rstrip('/')}/esearch.fcgi"
        params = {"db": "books", "term": "livertox[book]", "usehistory": "y", "retmode": "json"}

        logger.info(f"Executing E-Search on NCBI Books database: {params['term']}")
        response = self._execute_request("GET", url, params)

        try:
            data = response.json()
            esearchresult = data.get("esearchresult", {})

            # Extract essential tokens
            count = int(esearchresult.get("count", 0))
            query_key = esearchresult.get("querykey")
            webenv = esearchresult.get("webenv")

            if not query_key or not webenv:
                raise ValueError("E-Search response is missing 'querykey' or 'webenv'.")

            return {"count": count, "query_key": query_key, "webenv": webenv}
        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"Failed to parse E-Search response: {e}")
            raise EutilitiesAPIError(f"Failed to parse E-Search response: {e}") from e

    def execute_efetch_pagination(self, count: int, query_key: str, webenv: str) -> Generator[str]:
        """
        AGENT INSTRUCTION: Execute the E-Fetch capability in a paginated loop.
        Yields raw XML strings for each chunk based on the configured `retmax`.
        """
        url = f"{self.config.base_url.rstrip('/')}/efetch.fcgi"
        retmax = self.config.retmax

        logger.info(f"Starting E-Fetch pagination. Total expected records: {count}. Batch size: {retmax}")

        for retstart in range(0, count, retmax):
            params = {
                "db": "books",
                "query_key": query_key,
                "WebEnv": webenv,
                "retstart": retstart,
                "retmax": retmax,
                "retmode": "xml",
            }

            logger.info(f"Fetching batch: start={retstart}, max={retmax}")
            response = self._execute_request("GET", url, params)

            # Yield raw XML text to be consumed by the StructuralXMLTransmutationTask
            yield response.text
