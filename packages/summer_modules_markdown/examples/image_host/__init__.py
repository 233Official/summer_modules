"""Image host example helpers."""

from pathlib import Path
from typing import Any, Dict

from .. import load_example_config

CURRENT_DIR = Path(__file__).parent.resolve()


def get_image_host_config() -> Dict[str, Any]:
    config = load_example_config("image_host")
    return config if isinstance(config, dict) else {}
