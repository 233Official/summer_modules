import base64
import hashlib
from pathlib import Path
from typing import Any, Dict

import pytest

from summer_modules_bot.wxwork.wxworkbot import WXWorkBot


@pytest.fixture
def capture_httpx_post(monkeypatch):
    calls: list[Dict[str, Any]] = []

    def fake_post(url, *, headers=None, json=None):
        calls.append({"url": url, "headers": headers or {}, "json": json or {}})
        class Response:
            status_code = 200

        return Response()

    monkeypatch.setattr("httpx.post", fake_post)
    return calls


def test_post_md_message(capture_httpx_post):
    bot = WXWorkBot("https://example.com/webhook")

    bot.post_md_message("hello")

    assert len(capture_httpx_post) == 1
    call = capture_httpx_post[0]
    assert call["url"] == "https://example.com/webhook"
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["json"]["msgtype"] == "markdown"
    assert call["json"]["markdown"]["content"] == "hello"


def test_post_md_message_v2(capture_httpx_post):
    bot = WXWorkBot("https://example.com/webhook")

    bot.post_md_message_v2("hello")

    payload = capture_httpx_post[-1]["json"]
    assert payload["msgtype"] == "markdown_v2"
    assert payload["markdown_v2"]["content"] == "hello"


def test_post_img_from_path(tmp_path: Path, capture_httpx_post):
    img_path = tmp_path / "img.bin"
    img_path.write_bytes(b"image-bytes")

    bot = WXWorkBot("https://example.com/webhook")
    bot.post_img_from_path(img_path)

    payload = capture_httpx_post[-1]["json"]
    assert payload["msgtype"] == "image"
    image_data = payload["image"]
    assert image_data["base64"] == base64.b64encode(b"image-bytes").decode("utf-8")
    assert image_data["md5"] == hashlib.md5(b"image-bytes").hexdigest()


def test_post_md_message_from_path(tmp_path: Path, capture_httpx_post):
    md_path = tmp_path / "message.md"
    md_path.write_text("markdown content", encoding="utf-8")

    bot = WXWorkBot("https://example.com/webhook")
    bot.post_md_message_from_path(md_path)

    payload = capture_httpx_post[-1]["json"]
    assert payload["markdown"]["content"] == "markdown content"
