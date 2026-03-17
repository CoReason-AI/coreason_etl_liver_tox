# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module defines the strict Pydantic configurations for the application.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigurationSettings(BaseSettings):
    """
    AGENT INSTRUCTION: Strict Pydantic Configuration class mapping to environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    ncbi_eutils_base_url: str = Field(
        default="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
        description="The base URL for NCBI E-utilities.",
    )

    ncbi_api_key: str | None = Field(
        default=None,
        description="Optional API key for NCBI E-utilities to increase rate limits.",
    )

    ncbi_retmax: int = Field(
        default=100,
        ge=1,
        le=500,
        description="The maximum number of records to fetch per request. Must be between 1 and 500.",
    )

    livertox_book_id: str = Field(
        default="NBK547852",
        description="The NCBI Book ID for LiverTox.",
    )

    log_level: str = Field(
        default="INFO",
        description="The logging level.",
    )


settings = ConfigurationSettings()
