# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

"""
AGENT INSTRUCTION: This module serves as the orchestrator for the Bronze layer ingestion intent.
"""

import dlt

from coreason_etl_liver_tox.bronze_ingestion_intent import livertox_source
from coreason_etl_liver_tox.utils.logger import logger


class EpistemicPipelineExecutionIntent:
    """
    AGENT INSTRUCTION: Encapsulates the execution policy for orchestrating the dlt ingestion pipeline,
    extracting LiverTox monographs, and loading them into PostgreSQL via JSONB.
    """

    def __init__(self, pipeline_name: str = "coreason_etl_livertox_pipeline", dataset_name: str = "bronze") -> None:
        """Initializes the execution intent with pipeline and dataset names."""
        self.pipeline_name = pipeline_name
        self.dataset_name = dataset_name

    def execute(self) -> None:
        """
        Executes the dlt pipeline load operation.
        Raises SystemExit if the pipeline fails, ensuring the process returns a non-zero exit code.
        """
        logger.info(f"Initiating pipeline execution: {self.pipeline_name}")
        try:
            # Initialize the dlt pipeline targeting Postgres
            pipeline = dlt.pipeline(
                pipeline_name=self.pipeline_name,
                destination="postgres",
                dataset_name=self.dataset_name,
            )

            # Extract data using the configured LiverTox source
            source_data = livertox_source()

            # Load the data
            load_info = pipeline.run(source_data)

            logger.info(f"Pipeline executed successfully. Load info: {load_info}")

        except Exception as e:
            logger.exception(f"Fatal error during pipeline execution: {e}")
            raise SystemExit(1) from e


def main() -> None:
    """Entry point for the execution."""
    intent = EpistemicPipelineExecutionIntent()
    intent.execute()


if __name__ == "__main__":  # pragma: no cover
    main()
