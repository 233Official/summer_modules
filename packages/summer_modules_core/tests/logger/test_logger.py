import io
import logging
import uuid
from pathlib import Path

from summer_modules_core.logger import init_and_get_logger


def test_logger_writes_to_log_file(temp_logger):
    logger, log_file = temp_logger
    message = "core logger write test"

    logger.info(message)

    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert message in content

    readme = log_file.parent / "README.md"
    assert readme.exists()


def test_logger_emits_colored_info(tmp_path: Path):
    logger_name = f"summer_modules_core_colored_{uuid.uuid4().hex}"
    logger = init_and_get_logger(tmp_path, logger_name, enable_color=True)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)

    logger.info("colored info test", info_color="green")

    handler.flush()
    logger.removeHandler(handler)
    for h in logger.handlers:
        h.close()
    logger.handlers.clear()

    output = stream.getvalue()
    assert "colored info test" in output
