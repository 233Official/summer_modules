import base64
import hashlib
from pathlib import Path

import httpx

from summer_modules_bot import WXWORKBOT_LOGGER


class WXWorkBot:
    def __init__(self, webhook: str, logger=None):
        self.webhook = webhook
        if logger is None:
            self.logger = WXWORKBOT_LOGGER
        else:
            self.logger = logger

    def post_md_message(self, msg: str) -> None:
        """推送 markdown 信息到企微Bot
        :param msg: markdown格式的信息
        """
        self.logger.info("推送信息到企微Bot")
        self.logger.info(f'当前msg字节数为 {len(msg.encode("utf-8"))}')
        httpx.post(
            url=self.webhook,
            headers={
                "Content-Type": "application/json",
            },
            json={
                "msgtype": "markdown",
                "markdown": {"content": f"{msg}"},
            },
        )
        self.logger.info("推送信息到企微Bot完成")

    def post_md_message_v2(self, msg: str) -> None:
        """推送 markdown 信息到企微Bot
        :param msg: markdown格式的信息
        """
        self.logger.info("推送信息到企微Bot")
        self.logger.info(f'当前msg字节数为 {len(msg.encode("utf-8"))}')
        httpx.post(
            url=self.webhook,
            headers={
                "Content-Type": "application/json",
            },
            json={
                "msgtype": "markdown_v2",
                "markdown_v2": {"content": f"{msg}"},
            },
        )
        self.logger.info("推送信息到企微Bot完成")

    def post_img(self, base64_img: str, md5_img: str) -> None:
        """推送图片到企微Bot
        :param base64_img: 图片的base64编码
        :param md5_img: 图片的md5值
        """
        self.logger.info("推送图片到企微Bot")
        httpx.post(
            url=self.webhook,
            headers={
                "Content-Type": "application/json",
            },
            json={
                "msgtype": "image",
                "image": {
                    "base64": f"{base64_img}",
                    "md5": f"{md5_img}",
                },
            },
        )
        self.logger.info("推送图片到企微Bot完成")

    def post_md_message_from_path(self, markdown_file_path: Path) -> None:
        """从文件中读取markdown内容并推送到企微Bot
        :param markdown_file_path: markdown文件的路径
        """
        self.logger.info("从文件中读取markdown内容并推送到企微Bot")
        with open(markdown_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.post_md_message(content)
        self.logger.info("从文件中读取markdown内容并推送到企微Bot完成")

    def post_md_message_v2_from_path(self, markdown_file_path: Path) -> None:
        """从文件中读取markdown内容并推送到企微Bot
        :param markdown_file_path: markdown文件的路径
        """
        self.logger.info("从文件中读取markdown内容并推送到企微Bot")
        with open(markdown_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.post_md_message_v2(content)
        self.logger.info("从文件中读取markdown内容并推送到企微Bot完成")

    def post_img_from_path(self, img_path: Path) -> None:
        """推送图片到企微Bot
        :param img_path: 图片的路径
        """
        self.logger.info("推送图片到企微Bot")
        with open(img_path, "rb") as f:
            data = f.read()
            base64_img = base64.b64encode(data).decode("utf-8")
            md5_img = hashlib.md5(data).hexdigest()
            self.logger.info(f"图片的base64编码长度为 {len(base64_img)}")
            self.logger.info(f"图片的md5值为 {md5_img}")

        self.post_img(base64_img=base64_img, md5_img=md5_img)
        self.logger.info("从本地文件推送图片到企微Bot完成")
