from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import tomllib

from summer_modules_core.logger import init_and_get_logger

CURRENT_FILE = Path(__file__).resolve()
PACKAGE_ROOT = CURRENT_FILE.parent
SRC_ROOT = PACKAGE_ROOT.parent
PACKAGE_DIR = SRC_ROOT.parent
PACKAGES_ROOT = PACKAGE_DIR.parent
PROJECT_ROOT = PACKAGES_ROOT.parent

_PROJECT_PYPROJECT = PROJECT_ROOT / "pyproject.toml"
_IS_PROJECT_ROOT = False
if _PROJECT_PYPROJECT.exists():
    try:
        project_data = tomllib.loads(_PROJECT_PYPROJECT.read_text(encoding="utf-8"))
        _IS_PROJECT_ROOT = project_data.get("project", {}).get("name") == "summer-modules"
    except Exception:
        _IS_PROJECT_ROOT = False

RESOURCE_DIR = PACKAGE_ROOT / "resources"
summer_modules_core_logger = init_and_get_logger(
    PACKAGE_ROOT, "summer_modules_core_logger"
)


def load_config(section: str | None = None, *, package_root: Path | None = None) -> Dict[str, Any]:
    """Load configuration from project-wide or package-local config.toml."""

    search_paths = []
    if _IS_PROJECT_ROOT:
        search_paths.append(PROJECT_ROOT / "config.toml")
    root_for_package = package_root or PACKAGE_DIR
    search_paths.append(root_for_package / "config.toml")

    config_data: Dict[str, Any] = {}

    for candidate in search_paths:
        if candidate.exists():
            try:
                config_data = tomllib.loads(candidate.read_text(encoding="utf-8"))  # type: ignore[arg-type]
                break
            except Exception:
                continue

    if section is None:
        return config_data

    value = config_data.get(section, {})
    return value if isinstance(value, dict) else {}


__all__ = [
    "RESOURCE_DIR",
    "summer_modules_core_logger",
    "load_config",
    "PROJECT_ROOT",
    "PACKAGE_DIR",
]
