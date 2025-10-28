from __future__ import annotations

from pathlib import Path
import logging

def init_and_get_logger(
    current_dir: Path,
    logger_name: str = ...,
    *,
    enable_color: bool = ...,
) -> logging.Logger: ...

