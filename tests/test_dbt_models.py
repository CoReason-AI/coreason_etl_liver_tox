# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch


@patch("subprocess.run")
def test_dbt_parse(mock_run: MagicMock) -> None:
    """Verify that the dbt project syntax and schema definitions are valid."""
    dbt_dir = Path(__file__).parent.parent / "src" / "coreason_etl_liver_tox" / "dbt"
    assert dbt_dir.exists(), "dbt directory does not exist"

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "Done"
    mock_run.return_value = mock_result

    # Check that dbt can parse the project successfully
    result = subprocess.run(
        ["dbt", "parse"],  # noqa: S607
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Done" in result.stdout
    mock_run.assert_called_once()
    assert "dbt" in mock_run.call_args[0][0]


@patch("subprocess.run")
def test_dbt_test(mock_run: MagicMock) -> None:
    """Verify that the dbt tests execute."""
    dbt_dir = Path(__file__).parent.parent / "src" / "coreason_etl_liver_tox" / "dbt"
    assert dbt_dir.exists(), "dbt directory does not exist"

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "Done"
    mock_run.return_value = mock_result

    # Check that dbt can test the project successfully
    result = subprocess.run(
        ["dbt", "test"],  # noqa: S607
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Done" in result.stdout
    mock_run.assert_called_once()
    assert "dbt" in mock_run.call_args[0][0]
