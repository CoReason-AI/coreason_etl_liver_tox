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

import dlt
import polars as pl
import pytest

from coreason_etl_liver_tox.config import LiverToxConfig
from coreason_etl_liver_tox.pipeline import create_pipeline, livertox_records, livertox_source


@pytest.fixture
def config() -> LiverToxConfig:
    return LiverToxConfig(ncbi_base_url="https://mock.ncbi.gov", api_key="secret", retmax=1)


@patch("coreason_etl_liver_tox.pipeline.LiverToxClient")
def test_livertox_records_yields_df(mock_client_class: MagicMock, config: LiverToxConfig) -> None:
    """Verify that the resource correctly transforms XML into a Polars DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_client.esearch.return_value = {"webenv": "env_1", "query_key": "qk_1", "count": 1}

    mock_xml = b"""
    <BookDocumentSet>
        <BookDocument>
            <BookData id="12345">
                <Chapter>
                    <sec>
                        <title>Hepatotoxicity</title>
                        <p>Drug causes severe liver injury.</p>
                    </sec>
                </Chapter>
            </BookData>
        </BookDocument>
        <BookDocument>
            <PMID>67890</PMID>
            <BookData>
                <Chapter>
                    <sec>
                        <title>Hepatotoxicity</title>
                        <p>Drug causes mild liver injury.</p>
                    </sec>
                </Chapter>
            </BookData>
        </BookDocument>
        <BookDocument></BookDocument>
    </BookDocumentSet>
    """
    mock_client.iter_efetch.return_value = [mock_xml]

    # Run the generator
    # We must iterate over the DltResource manually or mock its unwrapping
    resource = livertox_records(config)
    results = list(resource)

    assert len(results) == 1
    df = results[0]
    assert isinstance(df, pl.DataFrame)

    # Ensure correct columns and data extraction
    assert list(df.columns) == ["coreason_id", "uid", "ingestion_ts", "raw_data"]
    assert len(df) == 2
    assert df["uid"][0] == "12345"
    assert df["uid"][1] == "67890"

    # Check that isolations happened before polars packaging
    raw_data = df["raw_data"][0]
    assert "data" in raw_data
    assert "_hepatotoxicity_summary" in raw_data["data"]
    assert "severe liver injury" in raw_data["data"]["_hepatotoxicity_summary"]


@patch("coreason_etl_liver_tox.pipeline.LiverToxClient")
def test_livertox_records_handles_esearch_failure(mock_client_class: MagicMock, config: LiverToxConfig) -> None:
    """Verify that the resource explicitly crashes and bubbles exception on esearch failure."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_client.esearch.side_effect = Exception("E-search timeout")

    with pytest.raises(Exception, match="E-search timeout"):
        # The exception triggers upon resource creation before yielding
        next(iter(livertox_records(config)))


@patch("coreason_etl_liver_tox.pipeline.LiverToxClient")
def test_livertox_records_handles_efetch_parse_failure(mock_client_class: MagicMock, config: LiverToxConfig) -> None:
    """Verify that the resource handles parse errors securely."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_client.esearch.return_value = {"webenv": "env_1", "query_key": "qk_1", "count": 1}

    mock_client.iter_efetch.return_value = [b"Invalid < Unclosed XML"]

    # dlt wraps exceptions in ResourceExtractionError
    with pytest.raises(dlt.extract.exceptions.ResourceExtractionError):
        list(livertox_records(config))


@patch("coreason_etl_liver_tox.pipeline.LiverToxClient")
def test_livertox_source(mock_client_class: MagicMock, config: LiverToxConfig) -> None:
    """Verify that the source wraps the resource correctly and sets nesting=0."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_client.esearch.return_value = {"webenv": "env_1", "query_key": "qk_1", "count": 1}

    # Access resources bound to the source
    source = dlt.source(livertox_source)(config)
    assert len(source.resources) == 1
    assert "bronze_livertox_raw" in source.resources
    # Check that nesting is forced disabled in metadata
    assert source.max_table_nesting == 0


def test_create_pipeline() -> None:
    """Verify the core pipeline wrapper instantiates properly."""
    pipeline = create_pipeline()
    assert isinstance(pipeline, dlt.Pipeline)
    assert pipeline.pipeline_name == "coreason_etl_liver_tox"
    assert pipeline.dataset_name == "bronze"
    assert pipeline.destination.destination_name == "postgres"
