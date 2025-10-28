from pathlib import Path
from typing import Any

import pytest

from summer_modules_markdown.oss.gitlab import GitlabOSS
from summer_modules_markdown.image_host.gitlab import GitlabImageHost


@pytest.fixture
def gitlab_oss(monkeypatch):
    monkeypatch.setattr(GitlabImageHost, "repo_dir_check", lambda self: True)
    oss = GitlabOSS(
        token="token",
        project_id="123",
        branch="main",
        repo_url="https://gitlab.com/example/repo",
        gitlab_base_url="https://gitlab.com",
    )
    return oss


def test_upload_text_file_creates_directory(gitlab_oss, tmp_path, monkeypatch):
    oss = gitlab_oss
    text_file = tmp_path / "note.txt"
    text_file.write_text("hello", encoding="utf-8")

    created_dirs: list[tuple[str, str]] = []
    created_files: list[dict[str, Any]] = []

    monkeypatch.setattr(oss, "is_dir_exists", lambda dir_path: False)
    monkeypatch.setattr(
        oss,
        "create_new_dir",
        lambda dir_path, commit_message: created_dirs.append((dir_path, commit_message))
        or True,
    )
    monkeypatch.setattr(
        oss,
        "create_new_file",
        lambda **kwargs: created_files.append(kwargs) or True,
    )

    url = oss.upload_text_file(text_file)

    assert created_dirs
    assert created_files
    payload = created_files[0]
    assert payload["content"] == "hello"
    assert url.endswith("note.txt")


def test_upload_text_file_missing_file(gitlab_oss):
    result = gitlab_oss.upload_text_file(Path("missing.txt"))
    assert result == ""
