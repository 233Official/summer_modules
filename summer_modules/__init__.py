from pathlib import Path
from summer_modules_core.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
RESOURCE_DIR = PROJECT_ROOT / "resources"


summer_modules_logger = init_and_get_logger(CURRENT_DIR, "summer_modules_loogger")


