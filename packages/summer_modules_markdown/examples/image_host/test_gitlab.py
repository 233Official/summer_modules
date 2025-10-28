"""GitLab 图床示例脚本。"""

from pathlib import Path
from typing import Any

from summer_modules_core.logger import init_and_get_logger

from summer_modules_markdown.image_host.gitlab import GitlabImageHost

from . import CURRENT_DIR, get_image_host_config

LOGGER = init_and_get_logger(CURRENT_DIR, "markdown_image_host_example")


def _resolve_config() -> dict[str, Any]:
    config = get_image_host_config().get("gitlab", {})
    if not isinstance(config, dict) or not config:
        raise RuntimeError("请在 config.toml 的 [image_host.gitlab] 中配置访问参数。")

    required = [
        "gitlab_token",
        "project_id",
        "branch",
        "repo_url",
        "gitlab_base_url",
    ]
    missing = [key for key in required if key not in config]
    if missing:
        raise RuntimeError(f"缺少配置项: {', '.join(missing)}")
    return config


def create_image_host() -> GitlabImageHost:
    cfg = _resolve_config()
    return GitlabImageHost(
        token=cfg["gitlab_token"],
        project_id=cfg["project_id"],
        branch=cfg["branch"],
        repo_url=cfg["repo_url"],
        gitlab_base_url=cfg["gitlab_base_url"],
        gitlab_repo_image_base_path=cfg.get("gitlab_repo_image_base_path", "pictures"),
        author_name=cfg.get("author_name"),
        author_email=cfg.get("author_email"),
    )


def upload_sample_image() -> None:
    host = create_image_host()
    image_path = (CURRENT_DIR / "laugh.jpg").resolve()
    url = host.upload_image(image_path=image_path)
    if url:
        LOGGER.info("图片上传成功: %s", url)
    else:
        LOGGER.error("图片上传失败")


def main() -> None:
    upload_sample_image()


if __name__ == "__main__":
    main()
