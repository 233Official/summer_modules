"""
使用 gitlab 仓库作为图床
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

from summer_modules_markdown.image_host import IMAGE_HOST_LOGGER


class GitlabImageHost:
    def __init__(
        self,
        token: str,
        project_id: str,
        branch: str,
        repo_url: str,
        gitlab_base_url: str,
        gitlab_repo_image_base_path: str = "pictures",
        timeout: Optional[int] = 20,
        author_email: Optional[str] = None,
        author_name: Optional[str] = "GitlabImageHost",
    ):
        """初始化Gitlab图床类

        Args:
            token (str): Gitlab API Token
            project_id (str): Gitlab项目ID
            branch (str): 分支名称
            repo_path (str): 仓库路径
            gitlab_base_url (str): Gitlab基础URL
            gitlab_repo_image_base_path (str): 仓库图片存储路径，默认为"pictures"

        Returns:
            None
        """
        self.token = token
        self.project_id = project_id
        self.branch = branch
        self.repo_url = repo_url
        self.gitlab_base_url = gitlab_base_url
        self.api_base_url = f"{gitlab_base_url}/api/v4/projects/{project_id}/repository"
        self.image_base_dir = gitlab_repo_image_base_path
        self.headers = {"PRIVATE-TOKEN": self.token}
        self.timeout = timeout
        self.author_name = author_name
        self.author_email = author_email

        # 检查仓库目录是否符合规范
        repo_dir_check_result = self.repo_dir_check()
        if not repo_dir_check_result:
            raise ValueError(
                f"仓库目录 {self.image_base_dir} 不符合规范, 请检查配置或手动创建该目录"
            )

    def repo_dir_check(self) -> bool:  # type: ignore
        """检查仓库目录是否符合规范
        检查仓库是否存在 self.image_base_path 目录, 不存在则创建
        Returns:
            bool: 如果目录存在或者创建成功则返回True, 否则返回False
        """
        # 如果 self.image_base_path 不包含 / 则说明应该查询项目根目录下是否存在 self.image_base_path
        is_image_base_dir_exists = self.is_dir_exists(dir_path=self.image_base_dir)
        if is_image_base_dir_exists:
            IMAGE_HOST_LOGGER.info(
                f"图像根目录 {self.image_base_dir} 已存在, 验证通过✅"
            )
            return True
        else:
            IMAGE_HOST_LOGGER.info(
                f"图像根目录 {self.image_base_dir} 不存在, 尝试创建中..."
            )
            create_result = self.create_new_dir(
                dir_path=self.image_base_dir,
                commit_message=f"创建图像根目录 {self.image_base_dir}",
            )
            if create_result:
                IMAGE_HOST_LOGGER.info(f"图像根目录 {self.image_base_dir} 创建成功✅")
                return True
            else:
                IMAGE_HOST_LOGGER.error(
                    f"图像根目录 {self.image_base_dir} 创建失败, 这种情况不应当发生❌, 请进行调试排查"
                )
                return False

    def is_dir_exists(self, dir_path: str) -> bool:
        """检查目录是否存在
        Args:
            dir_path (str): 目录路径
        Returns:
            bool: 如果目录存在则返回True, 否则返回False
        """
        dir_path_url_encoded = quote(dir_path, safe="")
        url = f"{self.api_base_url}/tree?path={dir_path_url_encoded}"

        with httpx.Client() as client:
            resp = client.get(url, headers=self.headers, timeout=self.timeout)
            if resp.status_code == 200:
                response_text = resp.json()
                # 如果返回的内容是一个列表且不为空，则目录存在
                if response_text:
                    IMAGE_HOST_LOGGER.info(f"目录 {dir_path} 已存在")
                    return True
                else:
                    IMAGE_HOST_LOGGER.info(f"目录 {dir_path} 不存在")
                    return False
            elif resp.status_code == 404:
                return False
            else:
                IMAGE_HOST_LOGGER.error(
                    f"检查目录 {dir_path} 是否存在失败: {resp.status_code} - {resp.text}"
                )
                return False

    def create_new_file(
        self,
        commit_message: str,
        content: str,
        file_path: str,
        execute_filemode: Optional[str] = None,
        start_branch: Optional[str] = None,
        encoding: Optional[str] = "text",
    ) -> bool:
        """创建新的文件
        Args:
            branch (str): 分支名称
            commit_message (str): 提交信息
            content (str): 文件内容
            file_path (str): 文件路径
            execute_filemode (Optional[str], optional): 执行文件模式. Defaults to None.
            start_branch (Optional[str], optional): 起始分支. Defaults to None.
            author_email (str, optional): 作者邮箱. Defaults to "".
            author_name (str, optional): 作者名称. Defaults to "".
            encoding (str, optional): 编码方式. Defaults to "text".(可以更改为 "base64")
        Returns:
            bool: 如果创建成功则返回True, 否则返回False
        """
        file_path_url_encoded = quote(file_path, safe="")
        url = f"{self.api_base_url}/files/{file_path_url_encoded}"
        headers = {
            "PRIVATE-TOKEN": self.token,
            "Content-Type": "application/json",
        }
        branch = self.branch
        data = {
            "branch": branch,
            "commit_message": commit_message,
            "content": content,
            "execute_filemode": execute_filemode,
            "start_branch": start_branch,
            "author_email": self.author_email,
            "author_name": self.author_name,
            "encoding": encoding,
        }
        with httpx.Client() as client:
            resp = client.post(url, headers=headers, json=data, timeout=self.timeout)
            if resp.status_code == 201:
                IMAGE_HOST_LOGGER.info(f"成功创建文件: {file_path}")
                return True
            elif resp.status_code == 400:
                IMAGE_HOST_LOGGER.error(f"创建文件失败: {resp.json()}")
                return False
            else:
                IMAGE_HOST_LOGGER.error(
                    f"创建文件失败: {resp.status_code} - {resp.text}"
                )
                return False

    def create_new_dir(
        self,
        dir_path: str,
        commit_message: str,
        execute_filemode: Optional[str] = None,
        start_branch: Optional[str] = None,
        encoding: Optional[str] = "text",
    ) -> bool:
        """创建新的目录
        Args:
            dir_path (str): 目录路径
            branch (str): 分支名称
            commit_message (str): 提交信息
            content (str): 文件内容
            execute_filemode (Optional[str], optional): 执行文件模式. Defaults to None.
            start_branch (Optional[str], optional): 起始分支. Defaults to None.
            author_email (str, optional): 作者邮箱. Defaults to "".
            author_name (str, optional): 作者名称. Defaults to "".
            encoding (str, optional): 编码方式. Defaults to "text".(可以更改为 "base64")
        Returns:
            bool: 如果创建成功则返回True, 否则返回False
        """
        file_path = f"{dir_path}/.gitkeep"  # 创建一个空文件以确保目录存在
        content = ""  # 空文件内容
        git_keep_create_result = self.create_new_file(
            commit_message=commit_message,
            content=content,
            file_path=file_path,
            execute_filemode=execute_filemode,
            start_branch=start_branch,
            encoding=encoding,
        )
        if git_keep_create_result:
            IMAGE_HOST_LOGGER.info(f"成功创建目录: {dir_path}")
            return True
        else:
            IMAGE_HOST_LOGGER.error(f"创建目录失败: {dir_path}")
            return False

    def upload_image(self, image_path: Path) -> str:
        """上传图片
        Args:
            image_path (Path): 图片路径
        Returns:
            str: 图片的URL
        """
        # 取图片文件名(包含扩展名)
        file_name = image_path.name
        current_time = datetime.now(ZoneInfo("Asia/Shanghai")).strftime(
            "%Y%m%d_%H%M%S%f"
        )
        current_year = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y")
        current_month = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%m")
        file_name = f"{current_time}_{file_name}"
        img_base_dir = f"{self.image_base_dir}/{current_year}/{current_month}"
        if not self.is_dir_exists(dir_path=img_base_dir):
            IMAGE_HOST_LOGGER.info(f"图片目录 {img_base_dir} 不存在, 尝试创建中...")
            self.create_new_dir(dir_path=img_base_dir, commit_message="创建图片目录")
        file_path = f"{img_base_dir}/{file_name}"

        # 读取图片内容并进行Base64编码
        with open(image_path, "rb") as img_file:
            content = base64.b64encode(img_file.read()).decode("utf-8")
        commit_message = f"上传图片: {file_name}"
        if self.create_new_file(
            commit_message=commit_message,
            content=content,
            file_path=file_path,
            encoding="base64",
        ):
            IMAGE_HOST_LOGGER.info(f"成功上传图片: {file_name}")
            # 返回图片的URL
            return f"{self.repo_url}/-/raw/{self.branch}/{file_path}"
        else:
            IMAGE_HOST_LOGGER.error(f"上传图片失败❌: {file_name}")
            return ""

    def delete_file(
        self,
        file_path: str,
        author_email: Optional[str] = None,
        author_name: Optional[str] = None,
        last_commit_id: Optional[str] = None,
        start_branch: Optional[str] = None,
    ) -> bool:
        """删除文件
        Args:
            commit_message (str): 提交信息
            file_path (str): 文件路径
            author_email (Optional[str], optional): 作者邮箱. Defaults to None.
            author_name (Optional[str], optional): 作者名称. Defaults to None.
            last_commit_id (Optional[str], optional): 最后提交ID. Defaults to None.
            start_branch (Optional[str], optional): 起始分支. Defaults to None.

        Returns:
            bool: 如果删除成功则返回True, 否则返回False
        """
        headers = {
            "PRIVATE-TOKEN": self.token,
            "Content-Type": "application/json",
        }
        data = {
            "branch": self.branch,
            "commit_message": f"🔥删除文件 - {file_path}",
            "author_email": author_email,
            "author_name": author_name,
            "last_commit_id": last_commit_id,
            "start_branch": start_branch,
        }
        file_path_url_encoded = quote(file_path, safe="")
        url = f"{self.api_base_url}/files/{file_path_url_encoded}"

        try:
            with httpx.Client() as client:
                resp = client.request(
                    "DELETE", url, headers=headers, json=data, timeout=self.timeout
                )
            if resp.status_code == 204:
                IMAGE_HOST_LOGGER.info(f"成功删除文件: {file_path}✅")
                return True
            elif resp.status_code == 400:
                IMAGE_HOST_LOGGER.error(f"删除文件失败: {resp.json()}❌")
                return False
            else:
                IMAGE_HOST_LOGGER.error(
                    f"删除文件失败: {resp.status_code} - {resp.text}❌"
                )
                return False
        except Exception as e:
            IMAGE_HOST_LOGGER.error(
                f"删除文件 {file_path} 时发生异常: {str(e)}\n{traceback.format_exc()}"
            )
            return False
