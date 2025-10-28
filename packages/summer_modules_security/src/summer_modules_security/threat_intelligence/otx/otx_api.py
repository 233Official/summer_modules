from __future__ import annotations

import atexit
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import httpx

from summer_modules_core.logger import init_and_get_logger
from summer_modules_core.utils import (
    read_json_file_to_dict,
    read_txt_file_to_list,
    write_dict_to_json_file,
    write_list_to_txt_file,
)
from summer_modules_core.web_request_utils import RetryableHTTPClient

from ..._storage import get_storage_dir
from .otx_api_model import GetPulsesActiveIOCsResponseModel, OTXIOCModel

OTX_BASE_URL = "https://otx.alienvault.com"
SEARCH_PULSE_URL = f"{OTX_BASE_URL}/api/v1/search/pulses"

DATA_DIR = get_storage_dir("threat_intelligence", "otx")
OTX_API_LOGGER = init_and_get_logger(DATA_DIR, "otx_api_logger")

PULSES_BASE_INFO_FILEPATH = DATA_DIR / "otx_pulses_base_info.json"
RECENTLY_MODIFIED_PULSES_BASE_INFO_FILEPATH = (
    DATA_DIR / "otx_recently_modified_pulses_base_info.json"
)
PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH = (
    DATA_DIR / "otx_pulses_subscriber_count_desc_sorted_id_list.txt"
)


class OTXApi:
    """封装 OTX API 调用逻辑的工具类。"""

    def __init__(
        self,
        otx_api_key: str,
        *,
        data_dir: Path | None = None,
        http_client: RetryableHTTPClient | None = None,
        auto_save_interval: int = 300,
        register_atexit: bool = True,
    ) -> None:
        """初始化 OTXApi。

        参数:
            otx_api_key: OTX 平台的 API Key。
            data_dir: 数据持久化目录，默认使用模块存储目录。
            http_client: 可注入的可重试 HTTP 客户端。
            auto_save_interval: 自动保存的时间间隔（秒），0 表示禁用。
            register_atexit: 是否注册退出钩子自动保存。
        """
        self.otx_api_key = otx_api_key
        self.data_dir = data_dir or DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.base_info_path = self.data_dir / PULSES_BASE_INFO_FILEPATH.name
        self.recent_path = self.data_dir / RECENTLY_MODIFIED_PULSES_BASE_INFO_FILEPATH.name
        self.subscriber_ids_path = (
            self.data_dir / PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH.name
        )

        self.last_save_time = 0.0
        self.modified = False
        self.auto_save_interval = max(auto_save_interval, 0)

        self.http_client = http_client or RetryableHTTPClient(
            logger=OTX_API_LOGGER, max_retries=3, retry_delay=1
        )

        self.init_pulses_base_info()

        if register_atexit:
            atexit.register(self.save_data)

    def init_pulses_base_info(self) -> None:
        """初始化本地缓存的脉冲基础信息。"""
        self.pulses_base_info = (
            read_json_file_to_dict(self.base_info_path)
            if self.base_info_path.exists()
            else {}
        )
        self.recently_modified_pulses_base_info = (
            read_json_file_to_dict(self.recent_path)
            if self.recent_path.exists()
            else {}
        )

        if self.subscriber_ids_path.exists():
            self.pulses_subscriber_count_desc_sorted_id_list = read_txt_file_to_list(
                self.subscriber_ids_path
            )
        elif self.pulses_base_info:
            self.pulses_subscriber_count_desc_sorted_id_list = sorted(
                self.pulses_base_info.keys(),
                key=lambda pid: self.pulses_base_info[pid]["subscriber_count"],
                reverse=True,
            )
            write_list_to_txt_file(
                self.pulses_subscriber_count_desc_sorted_id_list,
                self.subscriber_ids_path,
            )
        else:
            self.pulses_subscriber_count_desc_sorted_id_list = []

    def otx_search_pulses(
        self,
        limit: int = 10,
        page: int = 1,
        sort: str = "created",
        q: str = "",
        timeout: int = 30,
    ) -> Dict[str, Any]:
        """搜索 OTX 脉冲列表。"""
        if sort not in ["-modified", "modified", "-created", "created"]:
            raise ValueError(
                '排序字段无效, 可选项有 "-modified(最近修改)", "modified(最久修改)", "-created(最近创建)", "created(最久创建)"'
            )
        if page <= 0:
            raise ValueError("page必须是正整数")
        if page > 50:
            raise ValueError("OTX支持最多查询 50 页")
        if limit <= 0:
            raise ValueError("limit必须是正整数")
        if limit > 100:
            OTX_API_LOGGER.warning("limit超过100,自动限制为100")
            limit = 100

        params: Dict[str, Any] = {"limit": limit, "page": page, "sort": sort}
        if q:
            params["q"] = q

        headers = {"X-OTX-API-KEY": self.otx_api_key}
        response_json = self.http_client.get(
            url=SEARCH_PULSE_URL,
            headers=headers,
            params=params,
            timeout=timeout,
        )
        results = response_json.get("results", [])
        self.update_pulses_base_info(self.pulses_base_info, results)
        return response_json

    def update_pulses_base_info(
        self, pulses_base_info: dict, pulses_search_results: list
    ) -> None:
        """更新脉冲基础信息缓存。"""
        for pulse in pulses_search_results:
            pulses_base_info[pulse["id"]] = pulse

        self.pulses_subscriber_count_desc_sorted_id_list = sorted(
            pulses_base_info.keys(),
            key=lambda pid: pulses_base_info[pid]["subscriber_count"],
            reverse=True,
        )

        self.update_pulses_subscriber_count_desc_sorted_id_list(pulses_search_results)
        self.modified = True
        self.auto_save_if_needed()

    def update_pulses_subscriber_count_desc_sorted_id_list(
        self, pulses_search_results: list
    ) -> None:
        """同步订阅数降序排列的脉冲 ID 列表。"""
        sorted_pulses = sorted(
            pulses_search_results,
            key=lambda x: x["subscriber_count"],
            reverse=True,
        )
        source_sorted_pulses = self.pulses_subscriber_count_desc_sorted_id_list

        if not source_sorted_pulses:
            self.pulses_subscriber_count_desc_sorted_id_list = [
                pulse["id"] for pulse in sorted_pulses
            ]
            return

        merged_list: list[str] = []
        i = j = 0
        while i < len(source_sorted_pulses) and j < len(sorted_pulses):
            current_id = source_sorted_pulses[i]
            if (
                self.pulses_base_info[current_id]["subscriber_count"]
                >= sorted_pulses[j]["subscriber_count"]
            ):
                if current_id not in merged_list:
                    merged_list.append(current_id)
                i += 1
            else:
                candidate = sorted_pulses[j]["id"]
                if candidate not in merged_list:
                    merged_list.append(candidate)
                j += 1

        merged_list.extend(
            pid for pid in source_sorted_pulses[i:] if pid not in merged_list
        )
        merged_list.extend(
            pulse["id"] for pulse in sorted_pulses[j:] if pulse["id"] not in merged_list
        )

        self.pulses_subscriber_count_desc_sorted_id_list = merged_list

    def auto_save_if_needed(self) -> None:
        """在满足条件时自动保存数据。"""
        if not self.modified or not self.auto_save_interval:
            return
        current_time = time.time()
        if current_time - self.last_save_time > self.auto_save_interval:
            self.save_data()

    def save_data(self) -> None:
        """立即保存脉冲信息到本地。"""
        if not self.modified:
            return

        write_dict_to_json_file(
            self.pulses_base_info, self.base_info_path, one_line=True
        )
        write_dict_to_json_file(
            self.recently_modified_pulses_base_info,
            self.recent_path,
            one_line=True,
        )
        write_list_to_txt_file(
            data=self.pulses_subscriber_count_desc_sorted_id_list,
            filepath=self.subscriber_ids_path,
        )

        OTX_API_LOGGER.info(
            "数据已保存到\n%s\n%s\n%s",
            self.base_info_path,
            self.recent_path,
            self.subscriber_ids_path,
        )
        self.last_save_time = time.time()
        self.modified = False

    def otx_search_recently_modified_5000_pulses(self) -> list:
        """拉取最近修改的 5000 条脉冲信息。"""
        limit = 100
        page = 1
        sort = "-modified"
        interval = 3
        count = 50

        otx_recently_modified_5000_pulses_dict: dict[str, dict] = {}
        for i in range(count):
            OTX_API_LOGGER.info("当前正在查询第%s页，共%s页，每页%s条数据", i + 1, count, limit)
            response_dict = self.otx_search_pulses(
                limit=limit, page=page, sort=sort, timeout=100
            )
            for pulse in response_dict.get("results", []):
                otx_recently_modified_5000_pulses_dict[pulse["id"]] = pulse
            page += 1
            if i < count - 1:
                time.sleep(interval)

        sorted_pulses = sorted(
            otx_recently_modified_5000_pulses_dict.values(),
            key=lambda x: x["subscriber_count"],
            reverse=True,
        )

        OTX_API_LOGGER.info("查询到%s个Pulses", len(sorted_pulses))

        self.update_pulses_base_info(
            self.recently_modified_pulses_base_info,
            sorted_pulses,
        )
        return sorted_pulses

    def get_pulses_info(self, pulse_id: str, timeout: int = 30) -> dict:
        """获取指定 Pulse 的详细信息。"""
        if not isinstance(pulse_id, str):
            raise ValueError("pulse_id必须是字符串")
        url = f"{OTX_BASE_URL}/api/v1/pulses/{pulse_id}"
        headers = {"X-OTX-API-KEY": self.otx_api_key}
        return self.http_client.get(url=url, headers=headers, timeout=timeout)

    def get_pulses_indicators_by_page(
        self,
        pulse_id: str,
        page: int = 1,
        limit: int = 100,
        timeout: int = 30,
    ) -> dict:
        """分页获取 Pulse 的指标数据。"""
        if not isinstance(pulse_id, str):
            raise ValueError("pulse_id必须是字符串")
        if page <= 0:
            raise ValueError("page必须是正整数")
        if limit <= 0:
            raise ValueError("limit必须是正整数")
        if limit > 100:
            OTX_API_LOGGER.warning("limit超过100,自动限制为100")
            limit = 100

        url = f"{OTX_BASE_URL}/otxapi/pulses/{pulse_id}/indicators/"
        params = {"sort": "-created", "limit": limit, "page": page}
        headers = {"X-OTX-API-KEY": self.otx_api_key}
        return self.http_client.get(
            url=url,
            headers=headers,
            params=params,
            timeout=timeout,
        )

    def get_pulses_indicators(self, pulse_id: str, timeout: int = 30) -> dict:
        """获取指定 Pulse 的全部指标数据。"""
        if not isinstance(pulse_id, str):
            raise ValueError("pulse_id必须是字符串")

        result: dict[str, Any] = {}

        response_json = self.get_pulses_indicators_by_page(
            pulse_id=pulse_id,
            page=1,
            limit=100,
            timeout=timeout,
        )

        total_count = response_json.get("count", 0)
        result["count"] = total_count
        all_indicators = response_json.get("results", [])

        if total_count > 100:
            OTX_API_LOGGER.info(
                "Pulse %s 的 Indicators 数量超过100, 需要进行分页查询", pulse_id
            )
            total_pages = (
                response_json["total_pages"]
                if "total_pages" in response_json
                else (total_count + 99) // 100
            )
            for page in range(2, total_pages + 1):
                response_json = self.get_pulses_indicators_by_page(
                    pulse_id=pulse_id,
                    page=page,
                    limit=100,
                    timeout=timeout,
                )
                all_indicators.extend(response_json.get("results", []))

        result["results"] = all_indicators
        return result

    def get_pulse_iocs(
        self,
        pulse_id: str,
        retry: int = 3,
        timeout: int = 30,
    ) -> Optional[GetPulsesActiveIOCsResponseModel]:
        """获取 Pulse 的活跃 IOC 列表。"""
        if not isinstance(pulse_id, str):
            raise ValueError("pulse_id必须是字符串")
        if not isinstance(retry, int) or retry <= 0:
            raise ValueError("retry必须为正整数")

        url = f"{OTX_BASE_URL}/api/v1/pulses/{pulse_id}/indicators"
        headers = {"X-OTX-API-KEY": self.otx_api_key}

        for attempt in range(retry):
            response = self.http_client.get(
                url=url,
                headers=headers,
                timeout=timeout,
            )
            if response:
                return GetPulsesActiveIOCsResponseModel.model_validate(response)
            time.sleep(1)
        return None

    def save_pulse_iocs(
        self,
        pulse_id: str,
        pulse_iocs: GetPulsesActiveIOCsResponseModel,
        *,
        output_dir: Path | None = None,
    ) -> Path:
        """将 Pulse 的 IOC 数据保存为 JSON 文件。"""
        target_dir = output_dir or (self.data_dir / "otx_iocs")
        target_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
        filepath = target_dir / f"{timestamp}_{pulse_id}_iocs.json"
        write_dict_to_json_file(pulse_iocs.model_dump(), filepath, one_line=False)
        OTX_API_LOGGER.info("已保存 Pulse %s 的 IOC 到 %s", pulse_id, filepath)
        return filepath


__all__ = ["OTXApi"]
