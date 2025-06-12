from pathlib import Path

from tests import SUMMER_MODULES_TEST_LOGGER
from tests.markdown.image_host import IMAGE_HOST_CONFIG, CURRENT_DIR

from summer_modules.markdown.image_host.gitlab import GitlabImageHost


GITLAB_IMAGE_HOST_CONFIG = IMAGE_HOST_CONFIG.get("gitlab", {})
TOKEN = GITLAB_IMAGE_HOST_CONFIG.get("gitlab_token")
PROJECT_ID = GITLAB_IMAGE_HOST_CONFIG.get("project_id")
BRANCH = GITLAB_IMAGE_HOST_CONFIG.get("branch")
REPO_URL = GITLAB_IMAGE_HOST_CONFIG.get("repo_url")
GITLAB_BASE_URL = GITLAB_IMAGE_HOST_CONFIG.get("gitlab_base_url")
GITLAB_REPO_IMAGE_BASE_PATH = GITLAB_IMAGE_HOST_CONFIG.get(
    "gitlab_repo_image_base_path", "pictures"
)
if not TOKEN or not PROJECT_ID or not BRANCH or not REPO_URL or not GITLAB_BASE_URL:
    raise ValueError("配置文件中 gitlab 配置项不完整")

if not GITLAB_IMAGE_HOST_CONFIG:
    raise ValueError("配置文件中未找到 gitlab 配置项")

gitlab_image_host = GitlabImageHost(
    token=TOKEN,
    project_id=PROJECT_ID,
    branch=BRANCH,
    repo_url=REPO_URL,
    gitlab_base_url=GITLAB_BASE_URL,
    gitlab_repo_image_base_path=GITLAB_REPO_IMAGE_BASE_PATH,
    # gitlab_repo_image_base_path="test_dir/pictures",
)
SUMMER_MODULES_TEST_LOGGER.info(
    f"已成功初始化 GitlabImageHost 实例: {gitlab_image_host}"
)


def test_is_dir_exists():
    # 检查一个存在的目录是否存在
    dir_path = "pictures"
    exists = gitlab_image_host.is_dir_exists(dir_path=dir_path)
    if exists:
        SUMMER_MODULES_TEST_LOGGER.info(f"目录 {dir_path} 已存在, 验证通过✅")
    else:
        SUMMER_MODULES_TEST_LOGGER.error(
            f"目录 {dir_path} 不存在, 实际上应该存在, 验证不通过❌"
        )

    # 检查一个存在的子目录是否存在
    dir_path_sub = "pictures/2024"
    exists_sub = gitlab_image_host.is_dir_exists(dir_path=dir_path_sub)
    if exists_sub:
        SUMMER_MODULES_TEST_LOGGER.info(f"子目录 {dir_path_sub} 已存在, 验证通过✅")
    else:
        SUMMER_MODULES_TEST_LOGGER.error(
            f"子目录 {dir_path_sub} 不存在, 实际上应该存在, 验证不通过❌"
        )

    # 检查一个不存在的目录是否存在
    dir_path_not_exists = "pictures/not_exists"
    exists_not_exists = gitlab_image_host.is_dir_exists(dir_path=dir_path_not_exists)
    if exists_not_exists:
        SUMMER_MODULES_TEST_LOGGER.error(
            f"目录 {dir_path_not_exists} 已存在, 实际上应该不存在, 验证不通过❌"
        )
    else:
        SUMMER_MODULES_TEST_LOGGER.info(
            f"目录 {dir_path_not_exists} 不存在, 验证通过✅"
        )


def test_create_new_file():
    file_name = "test_image.txt"
    file_content = "test content"
    file_path = f"test_dir/{file_name}"
    gitlab_image_host.create_new_file(
        commit_message=f"创建文件 {file_path}",
        content=file_content,
        file_path=file_path,
    )


def test_create_new_dir():
    dir_path = "test_dir"
    gitlab_image_host.create_new_dir(
        dir_path=dir_path,
        commit_message=f"创建目录 {dir_path}",
    )

    dir_path2 = "test_dir/sub_dir"
    gitlab_image_host.create_new_dir(
        dir_path=dir_path2,
        commit_message=f"创建目录 {dir_path2}",
    )


def test_upload_picture():
    image_path = (CURRENT_DIR / "laugh.jpg").resolve()
    url = gitlab_image_host.upload_image(image_path=image_path)
    if url:
        SUMMER_MODULES_TEST_LOGGER.info(f"图片上传成功✅, URL: {url}")
    else:
        SUMMER_MODULES_TEST_LOGGER.error("图片上传失败❌")


def test_delete_file():
    file_path = "test_dir/test_image.txt"
    gitlab_image_host.delete_file(
        file_path=file_path,
    )

    file_path2 = "test_dir/.gitkeep"
    gitlab_image_host.delete_file(
        file_path=file_path2,
    )

    file_path3 = "test_dir/sub_dir/.gitkeep"
    gitlab_image_host.delete_file(
        file_path=file_path3,
    )

    file_path4 = "test_dir/pictures/.gitkeep"
    gitlab_image_host.delete_file(
        file_path=file_path4,
    )


def main():
    # test_is_dir_exists()
    # test_create_new_file()
    # test_create_new_dir()
    # test_delete_file()

    test_upload_picture()


if __name__ == "__main__":
    main()
