from pathlib import Path
from summer_modules.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).resolve().parent
summer_modules_logger = init_and_get_logger(CURRENT_DIR, "summer_modules_loogger")