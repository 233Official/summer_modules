#!/usr/bin/env python3
import runpy
import sys
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]


def file_path_to_module(file_path: str | Path) -> str:
    path_obj = Path(file_path).resolve()
    module_path = path_obj.relative_to(WORKSPACE_ROOT).with_suffix("")
    return ".".join(module_path.parts)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: debug_module.py <path-to-python-file>")
    module = file_path_to_module(sys.argv[1])
    runpy.run_module(module, run_name="__main__")
