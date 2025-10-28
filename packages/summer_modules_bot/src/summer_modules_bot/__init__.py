from pathlib import Path

from summer_modules_core.logger import init_and_get_logger

PACKAGE_ROOT = Path(__file__).resolve().parent
WXWORKBOT_LOGGER = init_and_get_logger(PACKAGE_ROOT, "wxworkbot_logger")

from .wxwork.wxworkbot import WXWorkBot  # noqa: E402

__all__ = ["WXWORKBOT_LOGGER", "WXWorkBot"]
