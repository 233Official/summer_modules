from typing import Any, Optional, Union
from pathlib import Path
import logging

class CustomLogger(logging.Logger):
    """自定义的 Logger 类型，扩展了标准 logging.Logger"""

    def info(
        self, msg: object, *args: Any, info_color: Optional[str] = None, **kwargs: Any
    ) -> None: ...

def init_and_get_logger(
    log_dir: Union[str, Path], logger_name: str, **kwargs: Any
) -> CustomLogger: ...
