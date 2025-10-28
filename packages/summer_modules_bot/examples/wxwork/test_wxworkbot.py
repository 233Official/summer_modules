"""WXWorkBot 使用示例。

运行示例前请设置以下环境变量：

    export WXWORK_WEBHOOK=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
    # 可选：export WXWORK_IMAGE_PATH=/path/to/image.jpg

执行：

    python -m packages.summer_modules_bot.examples.wxwork.test_wxworkbot
"""

import os
from pathlib import Path
from typing import Optional

from summer_modules_core.logger import init_and_get_logger
from summer_modules_markdown import Markdown

from summer_modules_bot.wxwork.wxworkbot import WXWorkBot

from . import CURRENT_DIR, get_wxwork_config

LOGGER = init_and_get_logger(CURRENT_DIR, "wxworkbot_example_logger")
WX_CONFIG = get_wxwork_config()


def _get_webhook() -> str:
    webhook = WX_CONFIG.get("webhook") if isinstance(WX_CONFIG, dict) else None
    if not webhook:
        webhook = os.environ.get("WXWORK_WEBHOOK")
    if not webhook:
        raise RuntimeError("请在 config.toml 或环境变量中提供 WXWork webhook。")
    return webhook


def _get_image_path() -> Optional[Path]:
    if isinstance(WX_CONFIG, dict) and WX_CONFIG.get("image_path"):
        return Path(str(WX_CONFIG["image_path"]))
    env_path = os.environ.get("WXWORK_IMAGE_PATH")
    return Path(env_path) if env_path else None


def _create_bot() -> WXWorkBot:
    return WXWorkBot(webhook=_get_webhook(), logger=LOGGER)


def test_post_md_message() -> None:
    bot = _create_bot()
    bot.post_md_message("测试企微Bot推送markdown信息")
    bot.post_md_message(
        "测试企微Bot推送markdown信息，包含特殊字符：!@#$%^&*()_+{}|:\"<>?[];',./`~"
    )


def test_post_md_message_v2_from_path() -> None:
    bot = _create_bot()
    markdown_file = CURRENT_DIR / "test_markdown.md"
    md = Markdown(markdown_file_path=markdown_file)
    bot.post_md_message_v2(md.content)


def test_post_img() -> None:
    image_path = _get_image_path()
    if not image_path:
        raise RuntimeError("请在 config.toml 或环境变量中指定 WXWORK_IMAGE_PATH。")
    if not image_path.exists():
        raise FileNotFoundError(f"图片文件不存在: {image_path}")

    bot = _create_bot()
    bot.post_img_from_path(image_path)


def main() -> None:
    test_post_md_message()
    test_post_md_message_v2_from_path()
    # 如需发送图片，请取消下行注释并设置 WXWORK_IMAGE_PATH
    test_post_img()


if __name__ == "__main__":
    main()
