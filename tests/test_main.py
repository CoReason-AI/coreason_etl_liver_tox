# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_liver_tox.main import EpistemicPipelineExecutionIntent, main


@pytest.fixture
def mock_dlt_pipeline() -> Generator[tuple[MagicMock, MagicMock]]:
    with patch("coreason_etl_liver_tox.main.dlt.pipeline") as mock_pipeline:
        mock_instance = MagicMock()
        mock_pipeline.return_value = mock_instance
        yield mock_pipeline, mock_instance


@pytest.fixture
def mock_livertox_source() -> Generator[MagicMock]:
    with patch("coreason_etl_liver_tox.main.livertox_source") as mock_source:
        yield mock_source


def test_epistemic_pipeline_execution_success(
    mock_dlt_pipeline: tuple[MagicMock, MagicMock], mock_livertox_source: MagicMock
) -> None:
    """Test successful execution of the dlt pipeline."""
    mock_pipeline_class, mock_pipeline_instance = mock_dlt_pipeline
    mock_pipeline_instance.run.return_value = MagicMock()
    mock_source_data = MagicMock()
    mock_livertox_source.return_value = mock_source_data

    intent = EpistemicPipelineExecutionIntent()
    intent.execute()

    mock_pipeline_class.assert_called_once_with(
        pipeline_name="coreason_etl_livertox_pipeline",
        destination="postgres",
        dataset_name="bronze",
    )
    mock_livertox_source.assert_called_once()
    mock_pipeline_instance.run.assert_called_once_with(mock_source_data)


@pytest.mark.usefixtures("mock_livertox_source")
def test_epistemic_pipeline_execution_failure(mock_dlt_pipeline: tuple[MagicMock, MagicMock]) -> None:
    """Test pipeline execution handling of exceptions."""
    _mock_pipeline_class, mock_pipeline_instance = mock_dlt_pipeline
    mock_pipeline_instance.run.side_effect = Exception("Mocked pipeline error")

    intent = EpistemicPipelineExecutionIntent()

    with pytest.raises(SystemExit) as exc_info:
        intent.execute()

    assert exc_info.value.code == 1


@pytest.mark.usefixtures("mock_livertox_source")
def test_main_function(mock_dlt_pipeline: tuple[MagicMock, MagicMock]) -> None:
    """Test the main entry point."""
    main()
    mock_pipeline_class, mock_pipeline_instance = mock_dlt_pipeline
    mock_pipeline_class.assert_called_once()
    mock_pipeline_instance.run.assert_called_once()
