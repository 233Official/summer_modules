from pathlib import Path
from typing import Any, Dict

from .. import EXAMPLES_ROOT, get_example_config

CURRENT_DIR = EXAMPLES_ROOT / "hbase"


def get_hbase_config() -> Dict[str, Any]:
    config = get_example_config("database")
    hbase_cfg = config.get("hbase") if isinstance(config, dict) else None
    return hbase_cfg if isinstance(hbase_cfg, dict) else {}


__all__ = ["CURRENT_DIR", "get_hbase_config"]
