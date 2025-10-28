import base64
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from zoneinfo import ZoneInfo

from summer_modules_markdown.image_host.gitlab import GitlabImageHost


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2025, 1, 15, 12, 0, tzinfo=tz or ZoneInfo("Asia/Shanghai"))


@pytest.fixture
def gitlab_host(monkeypatch):
    monkeypatch.setattr(GitlabImageHost, "repo_dir_check", lambda self: True)
    host = GitlabImageHost(
        token="token",
        project_id="123",
        branch="main",
        repo_url="https://gitlab.com/example/repo",
        gitlab_base_url="https://gitlab.com",
    )
    return host, monkeypatch


def test_upload_image_constructs_payload(gitlab_host, tmp_path, monkeypatch):
    host, monkeypatch = gitlab_host
    image_file = tmp_path / "avatar.png"
    image_file.write_bytes(b"fake-bytes")

    created_dirs: list[tuple[str, str]] = []
    created_files: list[dict[str, Any]] = []

    monkeypatch.setattr(host, "is_dir_exists", lambda dir_path: False)
    monkeypatch.setattr(
        host,
        "create_new_dir",
        lambda dir_path, commit_message: created_dirs.append((dir_path, commit_message))
        or True,
    )
    monkeypatch.setattr(
        host,
        "create_new_file",
        lambda **kwargs: created_files.append(kwargs) or True,
    )
    monkeypatch.setattr(
        "summer_modules_markdown.image_host.gitlab.datetime", FixedDateTime
    )

    url = host.upload_image(image_path=image_file)

    assert created_dirs
    dir_path, commit_msg = created_dirs[0]
    assert commit_msg == "创建图片目录"
    assert dir_path.endswith("/2025/01")

    assert created_files
    payload = created_files[0]
    assert payload["encoding"] == "base64"
    expected_content = base64.b64encode(b"fake-bytes").decode("utf-8")
    assert payload["content"] == expected_content
    assert url.endswith("avatar.png")


def test_is_dir_exists_handles_missing_directory(gitlab_host, monkeypatch):
    host, monkeypatch = gitlab_host

    class DummyResponse:
        def __init__(self, status_code, json_data=None, text=""):
            self.status_code = status_code
            self._json = json_data or []
            self.text = text

        def json(self):
            return self._json

    class DummyClient:
        def __init__(self, response):
            self.response = response
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None, timeout=None):
            self.calls.append((url, headers, timeout))
            return self.response

    dummy = DummyClient(DummyResponse(200, json_data=[]))
    monkeypatch.setattr(
        "summer_modules_markdown.image_host.gitlab.httpx.Client",
        lambda: dummy,
    )

    assert not host.is_dir_exists("missing")
    assert dummy.calls
