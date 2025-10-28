from pathlib import Path

from summer_modules_core.logger import init_and_get_logger

PACKAGE_ROOT = Path(__file__).parent.resolve()
HBASE_LOGGER = init_and_get_logger(PACKAGE_ROOT, "hbase")

__all__ = ["HBASE_LOGGER"]
