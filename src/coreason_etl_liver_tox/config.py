# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

"""Configuration schemas for the coreason_etl_liver_tox package."""

import os

from pydantic import BaseModel, Field


class LiverToxConfig(BaseModel):
    """Configuration model for the NCBI LiverTox API client.

    AGENT INSTRUCTION: Ensure validation rules (like maximum limits) adhere to NCBI E-utilities
    specifications.
    """

    ncbi_base_url: str = Field(
        default="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
        description="The base URL for NCBI E-utilities REST endpoints.",
    )
    api_key: str | None = Field(
        default=None,
        description="The NCBI API key. When present, rate limits are increased.",
    )
    retmax: int = Field(
        default=500,
        gt=0,
        le=10000,
        description="The maximum number of records to fetch in a single E-Fetch batch. "
        "Must be strictly greater than 0 and less than or equal to 10,000.",
    )

    @classmethod
    def from_env(cls) -> "LiverToxConfig":
        """Instantiate the configuration object, prioritizing environment variables.

        Returns:
            LiverToxConfig: A validated configuration instance.
        """
        env_vars: dict[str, str | int] = {}

        if "NCBI_BASE_URL" in os.environ:
            env_vars["ncbi_base_url"] = os.environ["NCBI_BASE_URL"]

        if "NCBI_API_KEY" in os.environ:
            env_vars["api_key"] = os.environ["NCBI_API_KEY"]

        if "NCBI_RETMAX" in os.environ:
            import contextlib

            with contextlib.suppress(ValueError):
                env_vars["retmax"] = int(os.environ["NCBI_RETMAX"])

        return cls(**env_vars)
