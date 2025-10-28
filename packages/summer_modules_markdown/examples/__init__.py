from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from summer_modules_core import load_config

EXAMPLES_ROOT = Path(__file__).resolve().parent
PACKAGE_DIR = EXAMPLES_ROOT.parent


def load_example_config(section: str) -> Dict[str, Any]:
    return load_config(section, package_root=PACKAGE_DIR)


__all__ = ["load_example_config"]
