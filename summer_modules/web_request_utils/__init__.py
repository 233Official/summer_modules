from pathlib import Path
import json
import random
import time
import httpx
from typing import Dict, Any, Optional, Callable, TypeVar, Union, List
import urllib.parse

CURRENT_DIR = Path(__file__).resolve().parent
USER_AGENT_FILEPATH = CURRENT_DIR / "browsers.json"


def getUserAgent():
    """读取 browsers.json 构造随机的 user-agent"""
    with open(USER_AGENT_FILEPATH, "r") as f:
        header = json.load(f)
    # print(type(header))
    browsers = header["browsers"]
    # print(f'{type(browsers)}')
    # 随机提取 browsers 中的一个键值对
    browser = random.choice(list(browsers.items()))
    return random.choice(browser[1])


# 用于装饰器返回类型
T = TypeVar("T")


class RetryableHTTPClient:
    """
    具有自动重试功能的HTTP客户端封装
    """

    def __init__(
        self,
        logger=None,
        max_retries: int = 3,
        retry_delay: int = 3,
        backoff_factor: float = 1.0,
    ):
        """
        初始化可重试的HTTP客户端

        :param logger: 日志记录器实例
        :param max_retries: 最大重试次数
        :param retry_delay: 初始重试延迟(3秒)
        :param backoff_factor: 重试延迟的指数增长因子
        """
        self.logger = logger
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor

    def request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str] | None = None,
        params: Dict[str, Any] | None = None,
        json: Dict[str, Any] | None = None,
        data: Any = None,
        timeout: int = 30,
        success_codes: List[int] | None = None,
        description: str = "",
    ) -> Dict[str, Any]:
        """
        发送HTTP请求并自动处理重试

        :param method: HTTP方法(GET, POST等)
        :param url: 请求URL
        :param headers: 请求头
        :param params: URL参数
        :param json: JSON请求体
        :param data: 表单数据或其他请求体
        :param timeout: 请求超时时间(秒)
        :param success_codes: 被视为成功的HTTP状态码列表(默认仅200)
        :param description: 请求描述(用于日志)
        :return: 响应JSON或空结果
        """
        success_codes = success_codes or [200]
        retry_count = 0
        current_delay = self.retry_delay
        response = None
        request_desc = description or f"{method} {url} 参数: {params} 数据: {data}"

        while retry_count < self.max_retries:
            try:
                with httpx.Client() as client:
                    response = client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=params,
                        json=json,
                        data=data,
                        timeout=timeout,
                    )

                if response.status_code in success_codes:
                    if self.logger:
                        self.logger.info(
                            f"请求成功: {request_desc}",
                            info_color=(
                                "magenta"
                                if hasattr(self.logger, "info")
                                and "info_color"
                                in self.logger.info.__code__.co_varnames
                                else None
                            ),
                        )
                    return response.json()
                else:
                    if self.logger:
                        self.logger.error(
                            f"请求失败, 状态码: {response.status_code}, 重试中... ({retry_count + 1}/{self.max_retries})"
                        )
            except httpx.TimeoutException:
                if self.logger:
                    self.logger.warning(
                        f"请求超时: {request_desc}, 重试中... ({retry_count + 1}/{self.max_retries})"
                    )
            except Exception as e:
                if self.logger:
                    self.logger.error(
                        f"请求异常: {request_desc}, 错误: {e}, 重试中... ({retry_count + 1}/{self.max_retries})"
                    )

            retry_count += 1
            if retry_count < self.max_retries:
                # 使用指数退避策略
                time.sleep(current_delay)
                current_delay *= self.backoff_factor

        # 重试耗尽
        if self.logger:
            self.logger.error(f"尝试请求{self.max_retries}次依旧失败: {request_desc}")
        return {"results": []}

    def get(self, url, **kwargs) -> Dict[str, Any]:
        """GET请求快捷方法"""
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs) -> Dict[str, Any]:
        """POST请求快捷方法"""
        return self.request("POST", url, **kwargs)


def get_standard_domain_from_origin_domain(origin_domain):
    """域名标准化函数
    从域名/URL中提取标准化的域名
    提取方案为：
    1. 如果没有协议以及URL PATH 的话直接操作域名字符串去除 www 顶级域名
    2. 如果有协议, 使用urlparse解析出域名后移除端口号
    3. 去除域名前面的www.前缀和空白, 并转换为小写形式

    参数:
        origin_domain (str): 原始域名或URL, 可以是完整URL或仅域名部分

    返回:
        str: 标准化后的域名, 不包含www前缀、协议、端口号和路径

    示例:
        >>> get_standard_domain_from_origin_domain("www.example.com")
        "example.com"

        >>> get_standard_domain_from_origin_domain("https://www.example.com:8080/path")
        "example.com"
    """
    # 快速路径：处理简单情况
    if not origin_domain or ("/" not in origin_domain and ":" not in origin_domain):
        return origin_domain.strip(" .").removeprefix("www.").lower()

    # 如果包含协议, 使用urlparse
    if "://" in origin_domain:
        try:
            parsed_url = urllib.parse.urlparse(origin_domain)
            domain = parsed_url.netloc or parsed_url.path.split("/")[0]
            domain = domain.split(":")[0]  # 移除端口号
        except:
            domain = origin_domain.split("/")[-1]
    else:
        # 没有协议但可能有路径
        domain = origin_domain.split("/")[0].split(":")[0]

    return domain.strip(" .").removeprefix("www.").lower()
