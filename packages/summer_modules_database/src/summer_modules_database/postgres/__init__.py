from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from summer_modules.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).parent.resolve()

POSTGRES_LOGGER = init_and_get_logger(current_dir=CURRENT_DIR, logger_name="postgres")
