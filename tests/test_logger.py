# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

from pathlib import Path

from coreason_etl_liver_tox.utils.logger import logger


def test_logger_setup() -> None:
    """Test the setup of the logger."""
    assert logger is not None
    assert Path("logs").exists()


def test_logger_mkdir(tmp_path: Path) -> None:
    """Test the setup of the logger when logs directory doesn't exist."""
    import importlib

    # Clean up the logger to avoid issues when reloading
    logger.remove()

    # Modify the source directly for testing as monkeypatching the Path
    # in module level execution requires tricky reloading
    with open("src/coreason_etl_liver_tox/utils/logger.py") as f:
        original_code = f.read()

    try:
        new_logs_dir = (tmp_path / "new_logs").as_posix()
        patched_code = original_code.replace('Path("logs")', f'Path("{new_logs_dir}")')
        patched_code = patched_code.replace('"logs/app.log"', f'"{new_logs_dir}/app.log"')

        with open("src/coreason_etl_liver_tox/utils/logger.py", "w") as f:
            f.write(patched_code)

        import coreason_etl_liver_tox.utils.logger as logger_module

        importlib.reload(logger_module)

        assert Path(new_logs_dir).exists()
    finally:
        with open("src/coreason_etl_liver_tox/utils/logger.py", "w") as f:
            f.write(original_code)

        # Reload again to restore the original state
        importlib.reload(logger_module)
