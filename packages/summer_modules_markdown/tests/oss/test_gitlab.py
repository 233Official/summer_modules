from pathlib import Path

from tests import SUMMER_MODULES_TEST_LOGGER
from tests.markdown.image_host import IMAGE_HOST_CONFIG
from tests.markdown.oss import CURRENT_DIR, OSS_CONFIG

from summer_modules.markdown.image_host.gitlab import GitlabImageHost
from summer_modules.markdown.oss.gitlab import GitlabOSS


GITLAB_IMAGE_HOST_CONFIG = IMAGE_HOST_CONFIG.get("gitlab", {})
TOKEN = GITLAB_IMAGE_HOST_CONFIG.get("gitlab_token")
PROJECT_ID = GITLAB_IMAGE_HOST_CONFIG.get("project_id")
BRANCH = GITLAB_IMAGE_HOST_CONFIG.get("branch")
REPO_URL = GITLAB_IMAGE_HOST_CONFIG.get("repo_url")
GITLAB_BASE_URL = GITLAB_IMAGE_HOST_CONFIG.get("gitlab_base_url")
GITLAB_REPO_IMAGE_BASE_PATH = GITLAB_IMAGE_HOST_CONFIG.get(
    "gitlab_repo_image_base_path", "pictures"
)
GITLAB_OSS_CONFIG = OSS_CONFIG["gitlab"]
GITLAB_REPO_OSS_BASE_PATH = GITLAB_OSS_CONFIG["gitlab_repo_oss_base_path"]
if (
    not TOKEN
    or not PROJECT_ID
    or not BRANCH
    or not REPO_URL
    or not GITLAB_BASE_URL
    or not GITLAB_REPO_OSS_BASE_PATH
):
    raise ValueError("配置文件中 gitlab 配置项不完整")

if not GITLAB_IMAGE_HOST_CONFIG:
    raise ValueError("配置文件中未找到 gitlab 配置项")
if not GITLAB_OSS_CONFIG:
    raise ValueError("配置文件中未找到 gitlab 相关的 oss 配置项")

gitlab_oss = GitlabOSS(
    token=TOKEN,
    project_id=PROJECT_ID,
    branch=BRANCH,
    repo_url=REPO_URL,
    gitlab_base_url=GITLAB_BASE_URL,
    gitlab_repo_image_base_path=GITLAB_REPO_IMAGE_BASE_PATH,
    gitlab_repo_oss_base_path=GITLAB_REPO_OSS_BASE_PATH,
)


def test_upload_text_file():
    text_file_path = CURRENT_DIR / "tmp/tmp.txt"
    url = gitlab_oss.upload_text_file(
        file_path=text_file_path,
    )
    if url:
        SUMMER_MODULES_TEST_LOGGER.info(
            f"文本文件上传成功, 文件 URL: {url}, 验证通过✅"
        )
    else:
        SUMMER_MODULES_TEST_LOGGER.error("文本文件上传失败, 验证不通过❌")


def main():
    test_upload_text_file()


if __name__ == "__main__":
    main()
