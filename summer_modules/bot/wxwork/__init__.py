from pathlib import Path
from summer_modules.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).resolve().parent
WXWORKBOT_LOGGER = init_and_get_logger(
    current_dir=CURRENT_DIR,
    logger_name="wxworkbot",
)
