from pathlib import Path
import toml

from summer_modules.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).parent.resolve()
SUMMER_MODULES_TEST_LOGGER = init_and_get_logger(
    current_dir=CURRENT_DIR, logger_name="summer_modules_test_logger"
)

CONFIG_FILEPATH = (CURRENT_DIR / "../config.toml").resolve()
if not CONFIG_FILEPATH.exists():
    raise FileNotFoundError(f"Config file not found: {CONFIG_FILEPATH}")
CONFIG = toml.load(CONFIG_FILEPATH)
