from pathlib import Path
from typing import Any, Dict

from summer_modules_core import load_config, init_and_get_logger

EXAMPLES_ROOT = Path(__file__).resolve().parent
PACKAGE_DIR = EXAMPLES_ROOT.parent
SUMMER_MODULES_DATABASE_EXAMPLE_LOGGER = init_and_get_logger(
    EXAMPLES_ROOT, "summer_modules_database_examples_logger"
)


def get_example_config(section: str) -> Dict[str, Any]:
    cfg = load_config(section, package_root=PACKAGE_DIR)
    return cfg if isinstance(cfg, dict) else {}


__all__ = ["get_example_config", "EXAMPLES_ROOT"]
