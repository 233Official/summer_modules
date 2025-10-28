from pathlib import Path

from summer_modules_core.logger import init_and_get_logger

PACKAGE_ROOT = Path(__file__).resolve().parent
DATABASE_LOGGER = init_and_get_logger(PACKAGE_ROOT, "database_logger")

__all__ = ["DATABASE_LOGGER"]
