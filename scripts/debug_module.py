#!/usr/bin/env python3
import json
import runpy
import sys
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
PYRIGHT_CONFIG = WORKSPACE_ROOT / "pyrightconfig.json"


def _bootstrap_sys_path() -> None:
    """根据 pyrightconfig.json 中的 extraPaths 配置同步更新 sys.path。"""
    extra_paths = []
    if PYRIGHT_CONFIG.exists():
        try:
            config_data = json.loads(PYRIGHT_CONFIG.read_text(encoding="utf-8"))
            extra_paths = config_data.get("extraPaths", [])
        except Exception:
            extra_paths = []

    candidate_paths = []
    for rel_path in extra_paths:
        candidate_paths.append((WORKSPACE_ROOT / rel_path).resolve())

    # 兜底加入典型源码目录
    candidate_paths.append((WORKSPACE_ROOT / "src").resolve())

    for path in candidate_paths:
        if path.exists():
            path_str = str(path)
            if path_str not in sys.path:
                sys.path.insert(0, path_str)


def file_path_to_module(file_path: str | Path) -> str:
    path_obj = Path(file_path).resolve()
    module_path = path_obj.relative_to(WORKSPACE_ROOT).with_suffix("")
    return ".".join(module_path.parts)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: debug_module.py <path-to-python-file>")
    _bootstrap_sys_path()
    module = file_path_to_module(sys.argv[1])
    runpy.run_module(module, run_name="__main__")
