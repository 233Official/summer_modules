from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Final

from prefect.logging import get_run_logger

LOG_FILE_NAME: Final = "basic.log"


class TimedRotatingFileHandler(RotatingFileHandler):
    """按大小轮转并为历史文件追加时间戳。"""

    def __init__(
        self,
        filename: Path | str,
        max_bytes: int = 1_000_000,
        backup_count: int = 5,
        encoding: str = "utf-8",
        delay: bool = False,
    ) -> None:
        super().__init__(
            str(filename),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding=encoding,
            delay=delay,
        )

    def doRollover(self) -> None:  # pragma: no cover - 依赖文件系统状态，测试中跳过
        if self.stream:
            self.stream.close()

        source = Path(self.baseFilename)
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        rotated = source.with_name(f"{source.stem}.{timestamp}.log")
        if source.exists():
            source.rename(rotated)

        if self.backupCount > 0:
            for path_str in self._getFilesToDelete():
                Path(path_str).unlink(missing_ok=True)

        if not self.delay:
            self.stream = self._open()

    def _getFilesToDelete(self) -> list[str]:
        directory = Path(self.baseFilename).parent
        base_stem = Path(self.baseFilename).stem
        candidates = sorted(
            str(path)
            for path in directory.glob(f"{base_stem}.*.log")
            if path.is_file()
        )
        if len(candidates) <= self.backupCount:
            return []
        return candidates[: len(candidates) - self.backupCount]


class CustomFormatter(logging.Formatter):
    """为控制台输出添加 ANSI 颜色。"""

    DATE_FMT = "%Y-%m-%d %H:%M:%S"
    BASE_FORMAT = "%(asctime)s - %(levelname)s: %(message)s (%(filename)s:%(lineno)d)"

    COLORS = {
        logging.DEBUG: "\x1b[90m",
        logging.INFO: "\x1b[34m",
        logging.WARNING: "\x1b[33m",
        logging.ERROR: "\x1b[31m",
        logging.CRITICAL: "\x1b[31;1m",
    }
    INFO_COLORS = {
        "default": "\x1b[34m",
        "green": "\x1b[32m",
        "yellow": "\x1b[33m",
        "magenta": "\x1b[35m",
        "cyan": "\x1b[36m",
    }
    RESET = "\x1b[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelno, self.RESET)
        if record.levelno == logging.INFO:
            color = self.INFO_COLORS.get(
                getattr(record, "info_color", "default"),
                self.INFO_COLORS["default"],
            )
        formatter = logging.Formatter(self.BASE_FORMAT, datefmt=self.DATE_FMT)
        return f"{color}{formatter.format(record)}{self.RESET}"


class PrefectLogHandler(logging.Handler):
    """把标准日志转发到 Prefect 运行日志。"""

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            prefect_logger = get_run_logger()
            message = logging.Formatter("%(message)s").format(record)
            log_method = getattr(prefect_logger, record.levelname.lower(), None)
            if callable(log_method):
                log_method(message)
            else:
                prefect_logger.info(message)


def init_and_get_logger(
    current_dir: Path,
    logger_name: str = "summer_prefect_logger",
    *,
    enable_color: bool = False,
) -> logging.Logger:
    """
    初始化 Prefect 辅助 logger。

    Args:
        current_dir: 用于存放日志文件的目录。
        logger_name: Logger 名称。
        enable_color: 是否启用彩色控制台输出。
    """
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    logs_dir = current_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    readme = logs_dir / "README.md"
    if not readme.exists():
        readme.write_text(
            "此目录用于存放 Prefect 相关日志。basic.log 为当前日志文件，"
            "basic.<timestamp>.log 为轮转后的历史文件。\n",
            encoding="utf-8",
        )

    file_handler = TimedRotatingFileHandler(logs_dir / LOG_FILE_NAME)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(CustomFormatter.BASE_FORMAT, datefmt=CustomFormatter.DATE_FMT)
    )
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    if enable_color:
        stream_handler.setFormatter(CustomFormatter())
    else:
        stream_handler.setFormatter(
            logging.Formatter(
                CustomFormatter.BASE_FORMAT, datefmt=CustomFormatter.DATE_FMT
            )
        )
    logger.addHandler(stream_handler)

    prefect_handler = PrefectLogHandler()
    prefect_handler.setLevel(logging.INFO)
    prefect_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(prefect_handler)

    return logger


__all__ = [
    "init_and_get_logger",
    "TimedRotatingFileHandler",
    "CustomFormatter",
]
