"""OSS example helpers."""

from pathlib import Path
from typing import Any, Dict

from .. import load_example_config

CURRENT_DIR = Path(__file__).parent.resolve()


def get_oss_config() -> Dict[str, Any]:
    config = load_example_config("oss")
    return config if isinstance(config, dict) else {}
