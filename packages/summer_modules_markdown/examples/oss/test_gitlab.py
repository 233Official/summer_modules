"""GitLab OSS 示例脚本。"""

from pathlib import Path
from typing import Any

from summer_modules_core.logger import init_and_get_logger

from summer_modules_markdown.image_host.gitlab import GitlabImageHost
from summer_modules_markdown.oss.gitlab import GitlabOSS

from .. import load_example_config
from . import CURRENT_DIR

LOGGER = init_and_get_logger(CURRENT_DIR, "markdown_oss_example")


def _resolve_configs() -> tuple[dict[str, Any], dict[str, Any]]:
    oss_config_root = load_example_config("oss")
    image_host_root = load_example_config("image_host")

    oss_config = (
        oss_config_root.get("gitlab", {}) if isinstance(oss_config_root, dict) else {}
    )
    image_host_config = (
        image_host_root.get("gitlab", {}) if isinstance(image_host_root, dict) else {}
    )

    if not isinstance(oss_config, dict) or not oss_config:
        raise RuntimeError("请在 config.toml 的 [oss.gitlab] 中配置访问参数。")
    if not isinstance(image_host_config, dict):
        image_host_config = {}

    return oss_config, image_host_config


def create_gitlab_oss() -> GitlabOSS:
    oss_cfg, image_cfg = _resolve_configs()
    token = oss_cfg.get("gitlab_token") or image_cfg.get("gitlab_token")
    project_id = oss_cfg.get("project_id") or image_cfg.get("project_id")
    branch = oss_cfg.get("branch") or image_cfg.get("branch")
    repo_url = oss_cfg.get("repo_url") or image_cfg.get("repo_url")
    gitlab_base_url = oss_cfg.get("gitlab_base_url") or image_cfg.get("gitlab_base_url")

    required = {
        "gitlab_token": token,
        "project_id": project_id,
        "branch": branch,
        "repo_url": repo_url,
        "gitlab_base_url": gitlab_base_url,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"缺少配置项: {', '.join(missing)}")

    return GitlabOSS(
        token=str(token),
        project_id=str(project_id),
        branch=str(branch),
        repo_url=str(repo_url),
        gitlab_base_url=str(gitlab_base_url),
        gitlab_repo_image_base_path=image_cfg.get(
            "gitlab_repo_image_base_path", "pictures"
        ),
        gitlab_repo_oss_base_path=oss_cfg.get("gitlab_repo_oss_base_path", "oss"),
        author_name=oss_cfg.get("author_name"),
        author_email=oss_cfg.get("author_email"),
    )


def upload_sample_text() -> None:
    oss_client = create_gitlab_oss()
    tmp_file = CURRENT_DIR / "tmp/tmp.txt"
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file.write_text("Hello OSS", encoding="utf-8")
    url = oss_client.upload_text_file(tmp_file)
    if url:
        LOGGER.info("文本上传成功: %s", url)
    else:
        LOGGER.error("文本上传失败")


def main() -> None:
    upload_sample_text()


if __name__ == "__main__":
    main()
