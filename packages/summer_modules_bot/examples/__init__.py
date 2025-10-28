from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import tomllib

EXAMPLES_ROOT = Path(__file__).resolve().parent
PACKAGE_ROOT = EXAMPLES_ROOT.parent
PROJECT_ROOT = PACKAGE_ROOT.parent.parent


def load_example_config(section: str) -> Dict[str, Any]:
    """按优先级加载配置: 根 config.toml -> 子包 config.toml -> 环境变量回退。"""
    candidates = [PROJECT_ROOT / "config.toml", PACKAGE_ROOT / "config.toml"]
    for candidate in candidates:
        if candidate.exists():
            data = tomllib.loads(candidate.read_text(encoding="utf-8"))
            if section in data:
                raw = data[section]
                return raw if isinstance(raw, dict) else {}
    return {}
