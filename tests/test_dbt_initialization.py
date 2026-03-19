# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module verifies the initialization of the DBT project and schema
by loading and checking for required constraints on core identifiers.
"""

from pathlib import Path

import yaml


def test_dbt_project_initialization() -> None:
    """Verifies that the dbt_project.yml exists and has the correct name."""
    project_path = Path("src/coreason_etl_liver_tox/dbt_project/dbt_project.yml")
    assert project_path.exists(), "dbt_project.yml missing."

    with open(project_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert config["name"] == "coreason_etl_liver_tox"
    assert config["config-version"] == 2


def test_dbt_schema_validation() -> None:
    """Verifies that the dbt schema.yml includes strict unique and not_null data tests."""
    schema_path = Path("src/coreason_etl_liver_tox/dbt_project/models/schema.yml")
    assert schema_path.exists(), "schema.yml missing."

    with open(schema_path, encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    # Validate structure
    assert "sources" in schema
    bronze_source = next((s for s in schema["sources"] if s["name"] == "bronze"), None)
    assert bronze_source is not None, "Missing 'bronze' source definition."

    table = next(
        (t for t in bronze_source["tables"] if t["name"] == "coreason_etl_liver_tox_bronze_livertox_raw"), None
    )
    assert table is not None, "Missing 'coreason_etl_liver_tox_bronze_livertox_raw' table."

    # Validate Columns and constraints
    columns = {col["name"]: col.get("data_tests", []) for col in table["columns"]}

    # coreason_id
    assert "coreason_id" in columns
    assert "unique" in columns["coreason_id"]
    assert "not_null" in columns["coreason_id"]

    # uid
    assert "uid" in columns

    # Validate Silver model tests
    silver_model = next(
        (m for m in schema.get("models", []) if m["name"] == "coreason_etl_liver_tox_silver_livertox_records"), None
    )
    assert silver_model is not None, "Missing 'coreason_etl_liver_tox_silver_livertox_records' model."

    silver_cols = {col["name"]: col.get("data_tests", []) for col in silver_model["columns"]}

    # ncbi_uid
    assert "ncbi_uid" in silver_cols
    assert "unique" in silver_cols["ncbi_uid"]
    assert "not_null" in silver_cols["ncbi_uid"]

    # agent_name
    assert "agent_name" in silver_cols
    assert "not_null" in silver_cols["agent_name"]
