from __future__ import annotations

import os
from pathlib import Path

__all__ = ["get_storage_dir", "set_storage_root", "storage_root"]

_DEFAULT_ROOT = (
    Path(os.environ.get("SUMMER_MODULES_SECURITY_HOME", ""))
    if os.environ.get("SUMMER_MODULES_SECURITY_HOME")
    else Path.home() / ".summer-modules" / "security"
)
_STORAGE_ROOT = _DEFAULT_ROOT.expanduser()


def set_storage_root(path: Path | str) -> None:
    """设置安全模块运行时数据的根目录。"""
    global _STORAGE_ROOT
    _STORAGE_ROOT = Path(path).expanduser().resolve()


def storage_root() -> Path:
    """返回当前安全模块使用的数据根目录。"""
    return _STORAGE_ROOT


def get_storage_dir(*parts: str, create: bool = True) -> Path:
    """获取根目录下的子路径；默认在需要时自动创建。"""
    path = storage_root().joinpath(*parts)
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path
