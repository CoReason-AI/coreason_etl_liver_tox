# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module provides the HTTP client policy for NCBI E-utilities interaction.
"""

from typing import Any

import requests
from ratelimit import limits, sleep_and_retry

from coreason_etl_liver_tox.config.settings import settings
from coreason_etl_liver_tox.utils.logger import logger


class EpistemicEUtilitiesClientPolicy:
    """
    AGENT INSTRUCTION: Encapsulates the configuration and execution policies for interactions
    with the NCBI E-utilities History Server. Implements a rate-limited Token Bucket algorithm.
    """

    def __init__(self) -> None:
        """Initializes the EpistemicEUtilitiesClientPolicy with settings."""
        self.base_url = settings.ncbi_eutils_base_url
        self.api_key = settings.ncbi_api_key
        self.session = requests.Session()

    @sleep_and_retry
    @limits(calls=10 if settings.ncbi_api_key else 3, period=1)
    def _execute_request(self, endpoint: str, params: dict[str, Any]) -> requests.Response:
        """
        Executes a rate-limited HTTP GET request.
        """
        if self.api_key:
            params["api_key"] = self.api_key

        url = f"{self.base_url}/{endpoint}"
        logger.debug(f"Executing request to {url} with params {params}")

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response

    def esearch_history_manifold(self, db: str, term: str) -> dict[str, Any]:
        """
        Executes an E-Search query against the NCBI database, placing the results onto the History Server.
        """
        params = {
            "db": db,
            "term": term,
            "usehistory": "y",
            "retmode": "json",
        }
        response = self._execute_request("esearch.fcgi", params)
        return response.json()  # type: ignore

    def efetch_xml_transmutation(
        self,
        db: str,
        query_key: str,
        webenv: str,
        retstart: int,
        retmax: int,
    ) -> str:
        """
        Executes an E-Fetch query against the NCBI database, retrieving a batch of XML records.
        """
        params = {
            "db": db,
            "query_key": query_key,
            "WebEnv": webenv,
            "retstart": retstart,
            "retmax": retmax,
            "retmode": "xml",
        }
        response = self._execute_request("efetch.fcgi", params)
        return str(response.text)
