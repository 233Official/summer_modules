"""
使用 gitlab 仓库作为对象存储服务
API DOC: https://docs.gitlab.com/18.0/api/repository_files/#delete-existing-file-in-repository
"""

from pathlib import Path
import base64
import httpx
from typing import Optional
from urllib.parse import quote
import traceback
from zoneinfo import ZoneInfo
from datetime import datetime

from summer_modules.utils import read_text_file_to_string
from summer_modules.markdown.oss import OSS_LOGGER
from summer_modules.markdown.image_host.gitlab import GitlabImageHost


class GitlabOSS(GitlabImageHost):

    def __init__(
        self,
        token: str,
        project_id: str,
        branch: str,
        repo_url: str,
        gitlab_base_url: str,
        gitlab_repo_image_base_path: str = "pictures",
        gitlab_repo_oss_base_path: str = "oss",
        timeout: int | None = 20,
        author_email: str | None = None,
        author_name: str | None = "GitlabImageHost",
    ):
        """初始化 Gitlab OSS 实例

        Args:
            token (str): Gitlab 访问令牌
            project_id (str): Gitlab 项目 ID
            branch (str): Gitlab 分支名称
            repo_url (str): Gitlab 仓库 URL
            gitlab_base_url (str): Gitlab 基础 URL
            gitlab_repo_image_base_path (str, optional): Gitlab 仓库图片基础路径. Defaults to "pictures".
            gitlab_repo_oss_base_path (str, optional): Gitlab 仓库 OSS 基础路径. Defaults to "oss".
            timeout (int | None, optional): 请求超时时间. Defaults to 20.
            author_email (str | None, optional): 提交作者的电子邮件. Defaults to None.
            author_name (str | None, optional): 提交作者的名称. Defaults to "GitlabImageHost".
        """
        super().__init__(
            token,
            project_id,
            branch,
            repo_url,
            gitlab_base_url,
            gitlab_repo_image_base_path,
            timeout,
            author_email,
            author_name,
        )
        self.gitlab_repo_oss_base_path = gitlab_repo_oss_base_path

    def upload_text_file(self, file_path: Path) -> str:
        """上传文本文件到 Gitlab OSS

        Args:
            file_path (Path): 本地文件路径
        Returns:
            str: 上传后的文件 URL
        """
        if not file_path.exists():
            OSS_LOGGER.error(f"文件 {file_path} 不存在, 无法上传")
            return ""

        filename = file_path.name
        current_time = datetime.now(ZoneInfo("Asia/Shanghai")).strftime(
            "%Y%m%d_%H%M%S%f"
        )
        filename = f"{current_time}_{filename}"
        file_base_dir = f"{self.gitlab_repo_oss_base_path}/text"
        if not self.is_dir_exists(file_base_dir):
            self.create_new_dir(
                dir_path=file_base_dir, commit_message="Create text directory"
            )

        # 读取文本文件内容
        file_content = read_text_file_to_string(file_path)
        commit_message = f"Upload text file {filename} to OSS"
        gitlab_filepath = f"{file_base_dir}/{filename}"
        if self.create_new_file(
            commit_message=commit_message,
            content=file_content,
            file_path=gitlab_filepath,
        ):
            OSS_LOGGER.info(f"文件 {filename} 上传成功: {gitlab_filepath}")
            return f"{self.repo_url}/-/raw/{self.branch}/{gitlab_filepath}"
        else:
            OSS_LOGGER.error(f"文件 {filename} 上传失败: {gitlab_filepath}")
            return ""
