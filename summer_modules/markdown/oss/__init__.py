from pathlib import Path

from summer_modules.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).parent.resolve()
OSS_LOGGER = init_and_get_logger(
    current_dir=CURRENT_DIR, logger_name="oss_logger"
)
