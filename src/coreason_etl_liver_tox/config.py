# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FederatedIngestionConfigManifest(BaseSettings):
    """
    AGENT INSTRUCTION: This class defines the strict configuration schema for the NCBI E-utilities
    federation tasks. It enforces the presence of required environmental variables to ensure deterministic execution.
    """

    api_key: str = Field(
        ...,
        description="The NCBI E-utilities API key. Required to unlock the 10 req/sec tier.",
        min_length=1,
    )

    retmax: int = Field(
        default=500,
        description="The batch size for paginated E-Fetch requests. Defaults to 500.",
        gt=0,
        le=10000,
    )

    base_url: str = Field(
        default="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
        description="The base URL for the NCBI E-utilities API.",
    )

    model_config = SettingsConfigDict(
        env_prefix="LIVERTOX_", case_sensitive=False, env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )
