import logging
import uuid
from pathlib import Path
from typing import Iterator, Tuple

import pytest

from summer_modules_core.logger import init_and_get_logger


@pytest.fixture
def temp_logger(tmp_path: Path) -> Iterator[Tuple[logging.Logger, Path]]:
    logger_name = f"summer_modules_core_test_{uuid.uuid4().hex}"
    logger = init_and_get_logger(tmp_path, logger_name)
    log_file = tmp_path / "logs" / "basic.log"

    yield logger, log_file

    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()
    logging.getLogger(logger.name).handlers.clear()
