from pathlib import Path

from summer_modules_core.logger import init_and_get_logger

PACKAGE_ROOT = Path(__file__).resolve().parent
RESOURCE_DIR = PACKAGE_ROOT / "resources"
summer_modules_core_logger = init_and_get_logger(
    PACKAGE_ROOT, "summer_modules_core_logger"
)

__all__ = ["RESOURCE_DIR", "summer_modules_core_logger"]
