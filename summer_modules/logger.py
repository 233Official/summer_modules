# 自定义 logger
import logging
import time
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path


class TimedRotatingFileHandler(RotatingFileHandler):
    """自定义RotatingFileHandler以添加时间戳"""

    def __init__(
        self, filename, maxBytes=0, backupCount=0, encoding="utf-8", delay=False
    ):
        """指定编码格式为utf-8, 防止中文乱码"""
        super().__init__(
            filename,
            maxBytes=maxBytes,
            backupCount=backupCount,
            encoding=encoding,
            delay=delay,
        )

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        # 拼接原先的文件名(不包含后缀) + 时间 + .log 后缀
        new_log_filename = (
            f"{os.path.splitext(self.baseFilename)[0]}.{current_time}.log"
        )
        # new_log_filename = f"{self.baseFilename}.{current_time}"

        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, new_log_filename)

        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)

        if not self.delay:
            self.stream = self._open()


class CustomFormatter(logging.Formatter):
    """自定义Formatter, 用于输出彩色日志"""

    # 一些常用的颜色转义序列, 可在如下连接中查看: https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
    red = "\x1b[31m"
    bold_red = "\x1b[31;1m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    blue = "\x1b[34m"
    magenta = "\x1b[35m"
    cyan = "\x1b[36m"
    white = "\x1b[37m"
    gray = "\x1b[90m"
    bright_red = "\x1b[91m"
    bright_green = "\x1b[92m"
    bright_yellow = "\x1b[93m"
    bright_blue = "\x1b[94m"
    bright_magenta = "\x1b[95m"
    bright_cyan = "\x1b[96m"
    bright_white = "\x1b[97m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s - %(levelname)s: %(message)s (%(filename)s:%(lineno)d)"
    date_format = "%Y-%m-%d %H:%M:%S"  # 不包含毫秒的日期格式

    INFO_COLORS = {
        "default": blue,
        "green": green,
        "yellow": yellow,
        "blue": blue,
        "magenta": magenta,
        "cyan": cyan,
        "white": white,
        "gray": gray,
        "bright_blue": bright_blue,
    }

    FORMATS = {
        logging.DEBUG: gray,
        logging.INFO: INFO_COLORS,  # 使用字典来处理不同的INFO颜色
        logging.WARNING: yellow,
        logging.ERROR: red,
        logging.CRITICAL: bold_red,
    }

    def format(self, record):
        color = self.FORMATS.get(record.levelno, self.gray)
        if isinstance(color, dict):
            # INFO级别的特殊处理
            info_color = getattr(record, "info_color", "default")
            color = color.get(info_color, self.gray)
        log_fmt = color + self.format_str + self.reset
        formatter = logging.Formatter(log_fmt, datefmt=self.date_format)
        return formatter.format(record)


class ColoredInfoLogger(logging.Logger):
    """自定义Logger, 用于添加为 INFO 添加颜色"""

    def info(self, msg, info_color="default", *args, **kwargs):
        # 添加info_color属性
        super().info(msg, extra={"info_color": info_color}, *args, **kwargs)


def init_and_get_logger(current_dir: Path, logger_name="summer_logger") -> logging.Logger:
    # 设置自定义的Logger
    logging.setLoggerClass(ColoredInfoLogger)
    logger = logging.getLogger(logger_name)

    # 如果logger已经配置过，直接返回
    if logger.handlers:
        return logger

    level = logging.DEBUG
    logger.setLevel(level)
    # 禁止日志向上传播到父logger，防止重复输出
    logger.propagate = False

    # 清除之前可能添加的handlers
    logger.handlers = []

    # 设置日志文件路径
    LOG_BASIC_NAME = "basic.log"
    LOG_BASIC_DIR = current_dir / "logs"
    LOG_BASIC_PATH = LOG_BASIC_DIR / LOG_BASIC_NAME
    # 如果不存在logs文件夹则创建
    if not os.path.exists(LOG_BASIC_DIR):
        os.mkdir(LOG_BASIC_PATH.parent)
    # 如果没有 README.md 文件, 则新建该文件用于说明日志文件的格式
    if not os.path.exists(LOG_BASIC_DIR / "README.md"):
        with open(LOG_BASIC_DIR / "README.md", "w") as f:
            f.write(
                f"此文件夹用于存放日志文件, {LOG_BASIC_NAME} 为基本日志文件, {LOG_BASIC_NAME}.时间 为旧日志文件, 以此类推; 当日志文件大小超过1MB时, 会自动创建新的日志文件, 旧日志文件会被重命名为 {LOG_BASIC_NAME}.[当前时间].log\n\n"
            )

    # 创建自定义的文件处理器并设置级别为DEBUG
    fh = TimedRotatingFileHandler(LOG_BASIC_PATH, maxBytes=1000000, backupCount=5)
    fh.setLevel(level)
    fh.setFormatter(CustomFormatter())
    logger.addHandler(fh)

    # 创建控制台处理器并设置级别为DEBUG
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    return logger
