from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from summer_modules_core import load_config

EXAMPLES_ROOT = Path(__file__).resolve().parent
PACKAGE_DIR = EXAMPLES_ROOT.parent


def get_example_config(section: str) -> Dict[str, Any]:
    config = load_config(section, package_root=PACKAGE_DIR)
    return config if isinstance(config, dict) else {}


def get_ssh_config() -> Dict[str, Any]:
    return get_example_config("ssh")


def get_hbase_config() -> Dict[str, Any]:
    return get_example_config("hbase")


__all__ = ["get_example_config", "get_ssh_config", "get_hbase_config", "EXAMPLES_ROOT"]
