from pathlib import Path
from typing import Any, Dict

from .. import EXAMPLES_ROOT, get_example_config

CURRENT_DIR = EXAMPLES_ROOT / "postgres"


def get_postgres_config() -> Dict[str, Any]:
    config = get_example_config("database")
    postgres_cfg = config.get("postgres") if isinstance(config, dict) else None
    return postgres_cfg if isinstance(postgres_cfg, dict) else {}


__all__ = ["CURRENT_DIR", "get_postgres_config"]
