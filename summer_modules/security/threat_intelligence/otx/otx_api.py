from typing import Dict, Any
import httpx
import time
import atexit
from pathlib import Path
from summer_modules.logger import init_and_get_logger
from summer_modules.utils import (
    write_dict_to_json_file,
    read_json_file_to_dict,
    write_list_to_txt_file,
    read_txt_file_to_list,
)
from summer_modules.web_request_utils import RetryableHTTPClient
from datetime import datetime
from zoneinfo import ZoneInfo

CURRENT_DIR = Path(__file__).parent.resolve()
OTX_API_LOGGER = init_and_get_logger(CURRENT_DIR, "otx_api_logger")
OTX_BASE_URL = "https://otx.alienvault.com"
SEARCH_PULSE_URL = f"{OTX_BASE_URL}/api/v1/search/pulses"

# 每次查询的 json 保存在 data 目录下
DATA_DIR = (CURRENT_DIR / "data").resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
PULSES_BASE_INFO_FILEPATH = DATA_DIR / "otx_pulses_base_info.json"
RECENTLY_MODIFIED_PULSES_BASE_INFO_FILEPATH = (
    DATA_DIR / "otx_recently_modified_pulses_base_info.json"
)
PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH = (
    DATA_DIR / "otx_pulses_subscriber_count_desc_sorted_id_list.txt"
)


class OTXApi:

    def __init__(self, otx_api_key: str):
        self.otx_api_key = otx_api_key
        self.last_save_time = 0
        self.modified = False
        self.init_pulses_base_info()
        self.http_client = RetryableHTTPClient(
            logger=OTX_API_LOGGER, max_retries=3, retry_delay=1
        )
        # 注册退出时保存数据
        atexit.register(self.save_data)

    def init_pulses_base_info(self):
        """
        初始化OTX API的Pulses基本信息
        :return: None
        """
        # 读取 Pulses 的基本信息
        if not PULSES_BASE_INFO_FILEPATH.exists():
            self.pulses_base_info = {}
        else:
            self.pulses_base_info = read_json_file_to_dict(PULSES_BASE_INFO_FILEPATH)

        # 读取最近修改的5000个Pulses的基本信息
        if not RECENTLY_MODIFIED_PULSES_BASE_INFO_FILEPATH.exists():
            self.recently_modified_pulses_base_info = {}
        else:
            self.recently_modified_pulses_base_info = read_json_file_to_dict(
                RECENTLY_MODIFIED_PULSES_BASE_INFO_FILEPATH
            )

        # 读取订阅数降序排序的Pulses ID列表
        if not PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH.exists():
            # 如果 pulses_base_info 不为空则直接对其进行排序
            if self.pulses_base_info:
                self.pulses_subscriber_count_desc_sorted_id_list = sorted(
                    self.pulses_base_info.keys(),
                    key=lambda x: self.pulses_base_info[x]["subscriber_count"],
                    reverse=True,
                )
                write_list_to_txt_file(
                    self.pulses_subscriber_count_desc_sorted_id_list,
                    PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH,
                )
            else:
                # 如果 pulses_base_info 为空则初始化为空列表
                self.pulses_subscriber_count_desc_sorted_id_list = []
        else:
            self.pulses_subscriber_count_desc_sorted_id_list = read_txt_file_to_list(
                PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH
            )

    def update_pulses_base_info(
        self, pulses_base_info: dict, pulses_search_results: list
    ) -> None:
        """
        更新OTX API的Pulses基本信息
        :param pulses_base_info: OTX API的Pulses基本信息(并非特指self.pulses_base_info, 类中的两个 base_info 变量的结构是一样的，都是 pulses 基本信息),由于字典是可变类型，引用到函数参数里传引用会修改原字典
        :param pulses_search_results: OTX API的Pulses搜索结果的 results 字段
        :return: None
        """
        # 将pulses_search_results中的每个Pulse的基本信息添加到pulses_base_info中
        for pulse in pulses_search_results:
            pulse_id = pulse["id"]
            # 暂时没有长期分析的需求，所以这里直接进行覆盖更新
            pulses_base_info[pulse_id] = pulse

        # 对 self。pulses_base_info 按照订阅人数降序排序来更新 self.pulses_subscriber_count_desc_sorted_id_list
        self.pulses_subscriber_count_desc_sorted_id_list = sorted(
            pulses_base_info.keys(),
            key=lambda x: pulses_base_info[x]["subscriber_count"],
            reverse=True,
        )

        self.update_pulses_subscriber_count_desc_sorted_id_list(pulses_search_results)
        self.modified = True
        self.auto_save_if_needed()

    def update_pulses_subscriber_count_desc_sorted_id_list(
        self, pulses_search_results: list
    ):
        """
        更新OTX API的Pulses订阅数降序排序的ID列表
        遍历pulses_search_results中的每个Pulse,将其ID插入到self.pulses_subscriber_count_desc_sorted_id_list中
        :param pulses_search_results: OTX API的Pulses搜索结果的 results 字段
        :return: None
        """
        # 先将 pulses_search_results 按照订阅人数降序排序
        sorted_pulses = sorted(
            pulses_search_results,
            key=lambda x: x["subscriber_count"],
            reverse=True,
        )
        source_sorted_pulses = self.pulses_subscriber_count_desc_sorted_id_list
        # 如果 self.pulses_subscriber_count_desc_sorted_id_list 为空,则直接赋值
        if not self.pulses_subscriber_count_desc_sorted_id_list:
            self.pulses_subscriber_count_desc_sorted_id_list = [
                pulse["id"] for pulse in sorted_pulses
            ]
            return
        """
        现在两个列表都是降序排列好的列表,  sorted_pulses 是按照订阅人数降序排列的pulse基本信息列表
        self.pulses_subscriber_count_desc_sorted_id_list 是按照订阅人数降序排列的pulse id列表
        直接归并两个列表即可
        """
        merged_list = []
        i = j = 0
        while i < len(source_sorted_pulses) and j < len(sorted_pulses):
            if (
                self.pulses_base_info[source_sorted_pulses[i]]["subscriber_count"]
                >= sorted_pulses[j]["subscriber_count"]
            ):
                if source_sorted_pulses[i] not in merged_list:
                    merged_list.append(source_sorted_pulses[i])
                i += 1
            else:
                if sorted_pulses[j]["id"] not in merged_list:
                    merged_list.append(sorted_pulses[j]["id"])
                j += 1
        # 将剩余的元素添加到 merged_list 中
        while i < len(source_sorted_pulses):
            if source_sorted_pulses[i] not in merged_list:
                merged_list.append(source_sorted_pulses[i])
            i += 1
        while j < len(sorted_pulses):
            if sorted_pulses[j]["id"] not in merged_list:
                merged_list.append(sorted_pulses[j]["id"])
            j += 1

        # 更新 self.pulses_subscriber_count_desc_sorted_id_list
        self.pulses_subscriber_count_desc_sorted_id_list = merged_list

    def auto_save_if_needed(self):
        """如果数据被修改且距离上次保存超过5分钟则保存"""
        current_time = time.time()
        if self.modified and (current_time - self.last_save_time > 300):
            self.save_data()

    def save_data(self):
        """保存数据到文件"""
        if not self.modified:
            return

        write_dict_to_json_file(
            self.pulses_base_info, PULSES_BASE_INFO_FILEPATH, one_line=True
        )
        write_dict_to_json_file(
            self.recently_modified_pulses_base_info,
            RECENTLY_MODIFIED_PULSES_BASE_INFO_FILEPATH,
            one_line=True,
        )
        write_list_to_txt_file(
            data=self.pulses_subscriber_count_desc_sorted_id_list,
            filepath=PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH,
        )

        OTX_API_LOGGER.info(
            f"数据已保存到\n{PULSES_BASE_INFO_FILEPATH}\n {RECENTLY_MODIFIED_PULSES_BASE_INFO_FILEPATH}\n {PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH}"
        )
        self.last_save_time = time.time()
        self.modified = False

    def otx_search_pulses(
        self,
        limit: int = 10,
        page: int = 1,
        sort: str = "created",
        q: str = "",
        timeout: int = 30,
    ) -> Dict[str, Any]:
        """
        使用OTX API搜索Pulses
        :param limit: 每页返回的结果数量(最大为100,超过100会自动被限制为100)
        :param page: 页码(最多查询50页)
        :param sort: 排序字段,可选项有"(-)created", "(-)modified"
        :param q: 搜索关键词
        :param timeout: 请求超时时间,单位为秒
        :return: 返回搜索结果
        """
        if sort not in ["-modified", "modified", "-created", "created"]:
            raise ValueError(
                '排序字段无效, 可选项有 "-modified(最近修改)", "modified(最久修改)", "-created(最近创建)", "created(最久创建)"'
            )
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit必须是正整数")
        if not isinstance(page, int) or page <= 0:
            raise ValueError("page必须是正整数")
        if page > 50:
            raise ValueError("OTX支持最多查询 50 页")
        if limit > 100:
            OTX_API_LOGGER.warning("limit超过100,自动限制为100")
            limit = 100
        if not isinstance(q, str):
            raise ValueError("q必须是字符串")
        # 如果q为空字符串,则不添加q参数
        if not q:
            params = {
                "limit": limit,
                "page": page,
                "sort": sort,
            }
        else:
            params = {
                "limit": limit,
                "page": page,
                "sort": sort,
                "q": q,
            }
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

    def otx_search_recently_modified_5000_pulses(self) -> list:
        """查询最近修改的5000个Pulses
        100条 * 50 页，需要查询 50 次，每次间隔 3 秒
        """
        # 每次查询100条,查询50页
        limit = 100
        page = 1
        sort = "-modified"
        # 每次查询间隔3秒
        interval = 3
        # 查询次数
        count = 50

        otx_recently_modified_5000_pulses_dict = {}
        for i in range(count):
            OTX_API_LOGGER.info(
                f"当前正在查询第{i + 1}页，共{count}页，每页{limit}条数据"
            )
            response_dict = self.otx_search_pulses(
                limit=limit, page=page, sort=sort, timeout=100
            )
            # response_dict 中的 results 是一个列表,包含了查询到的Pulses
            response_dict_results = response_dict.get("results", [])
            # 由于查询是实时的，所以可能会有重复的Pulses，所以需要去重，直接通过字典的键值对来去重，新的覆盖旧的就可以了
            for pulse in response_dict_results:
                # 这里的pulse是一个字典,包含了Pulse的所有信息
                # 通过pulse["id"]来去重
                pulse_id = pulse["id"]
                otx_recently_modified_5000_pulses_dict[pulse_id] = pulse
            page += 1
            if i < count - 1:
                time.sleep(interval)

        # 将去重后的结果按照订阅人数递减排序
        otx_recently_modified_5000_pulses_list_subscriber_count_desc_sorted = sorted(
            otx_recently_modified_5000_pulses_dict.values(),
            key=lambda x: x["subscriber_count"],
            reverse=True,
        )

        OTX_API_LOGGER.info(
            f"查询到{len(otx_recently_modified_5000_pulses_list_subscriber_count_desc_sorted)}个Pulses"
        )

        # 更新最近修改的5000个Pulses的基本信息
        self.update_pulses_base_info(
            self.recently_modified_pulses_base_info,
            otx_recently_modified_5000_pulses_list_subscriber_count_desc_sorted,
        )

        return otx_recently_modified_5000_pulses_list_subscriber_count_desc_sorted

    def get_pulses_info(self, pulse_id: str, timeout: int = 30) -> dict:
        """
        获取指定Pulse的详细信息
        :param pulse_id: Pulse的ID
        :param timeout: 请求超时时间,单位为秒(默认30秒)
        :return: Pulse的详细信息
        """
        if not isinstance(pulse_id, str):
            raise ValueError("pulse_id必须是字符串")
        url = f"{OTX_BASE_URL}/api/v1/pulses/{pulse_id}"
        headers = {"X-OTX-API-KEY": self.otx_api_key}
        response_json = self.http_client.get(
            url=url,
            headers=headers,
            timeout=timeout,
        )
        return response_json

    def get_pulses_indicators_by_page(
        self,
        pulse_id: str,
        page: int = 1,
        limit: int = 100,
        timeout: int = 30,
    ) -> dict:
        """
        获取指定Pulse的Indicators，支持分页
        :param pulse_id: Pulse的ID
        :param page: 页码(默认1)
        :param limit: 每页返回的结果数量(最大为100,超过100会自动被限制为100)
        :param timeout: 请求超时时间,单位为秒(默认30秒)
        :return: Pulse的Indicators
        """
        if not isinstance(pulse_id, str):
            raise ValueError("pulse_id必须是字符串")
        if not isinstance(page, int) or page <= 0:
            raise ValueError("page必须是正整数")
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit必须是正整数")
        if limit > 100:
            OTX_API_LOGGER.warning("limit超过100,自动限制为100")
            limit = 100

        url = f"{OTX_BASE_URL}/otxapi/pulses/{pulse_id}/indicators/"
        params = {
            "sort": "-created",
            "limit": limit,
            "page": page,
        }
        headers = {"X-OTX-API-KEY": self.otx_api_key}
        response_json = self.http_client.get(
            url=url,
            headers=headers,
            params=params,
            timeout=timeout,
        )
        return response_json

    def get_pulses_indicators(self, pulse_id: str, timeout: int = 30) -> dict:
        """
        获取指定Pulse的Indicators
        :param pulse_id: Pulse的ID
        :param timeout: 请求超时时间,单位为秒(默认30秒)
        :return: Pulse的Indicators
        """
        if not isinstance(pulse_id, str):
            raise ValueError("pulse_id必须是字符串")
        # 不要下面用官方文档的 API 接口，查不出来 IPV4 Type 的 IOC
        # url = f"{OTX_BASE_URL}/api/v1/pulses/{pulse_id}/indicators"
        # 使用如下 url 获取到的是所有类型的 IOC，包括 IPV4 Type 的 IOC
        # url = f"{OTX_BASE_URL}/otxapi/pulses/{pulse_id}/indicators/?sort=-created&limit=10&page=1"
        # url = f"{OTX_BASE_URL}/otxapi/pulses/{pulse_id}/indicators/?sort=-created&limit=100&page=1"

        result = {}

        reponse_json = self.get_pulses_indicators_by_page(
            pulse_id=pulse_id,
            page=1,  # 默认第一页
            limit=100,  # 每页100条
            timeout=timeout,
        )

        # 当前限制分页大小为100条，response_json 中的 count 字段如果大于100，则需要继续获取
        total_count = reponse_json.get("count", 0)
        result["count"] = total_count
        if total_count > 100:
            OTX_API_LOGGER.info(
                f"Pulse {pulse_id} 的 Indicators 数量超过100, 需要进行分页查询"
            )
            all_indicators = []
            for page in range(1, (total_count // 100) + 2):
                OTX_API_LOGGER.info(
                    f"正在获取 Pulse {pulse_id} 的 Indicators 第 {page} 页"
                )
                page_response = self.get_pulses_indicators_by_page(
                    pulse_id=pulse_id,
                    page=page,
                    limit=100,
                    timeout=timeout,
                )
                all_indicators.extend(page_response.get("results", []))
            # 将所有分页的结果合并
            result["indicators"] = all_indicators
        else:
            # 如果不超过100条，则直接返回结果
            result["indicators"] = reponse_json.get("results", [])

        return result

    def validate_ioc(self, ioc_info: dict) -> int:
        """验证 ioc 信息是否有效

        Args:
            ioc_info (dict): IOC 信息字典
        Returns:
            int:
            - 如果 IOC 信息有效则返回 1
            - 如果 IOC 因为 is_active 为 Flase 而无效则返回 0
            - 如果 IOC 因为 created 超过 1 年而无效则返回 -1
        """
        # 判断 ioc.is_active 是否为 True
        if not ioc_info.get("is_active", False):
            # OTX_API_LOGGER.debug(
            #     f"IOC {ioc_info.get('indicator', '未知')} is_active 为 False, 不符合活跃 IOC 的标准"
            # )
            return 0
        # 判断 ioc.created 是否在 1 年内
        created_str = ioc_info.get("created")
        if not created_str:
            OTX_API_LOGGER.error(
                f"IOC {ioc_info.get('indicator', '未知')} 的 created 字段缺失, 无法判断是否在 1 年内, 理论上应该不存在这种情况, created 应该是 ioc 必填字段"
            )
            return 0
        # ioc created 为 UTC 时间字符串(例如:2025-04-10T19:46:41), 将其转换为 datetime 对象然后与当前时间进行比较
        now = datetime.now(ZoneInfo("UTC"))
        ioc_created_time = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=ZoneInfo("UTC")
        )
        # 判断 ioc_created_time 是否在 1 年内
        if (now - ioc_created_time).days > 365:
            OTX_API_LOGGER.debug(
                f"IOC {ioc_info.get('indicator', '未知')} 的 created 时间 {ioc_created_time} 超过 1 年, 不符合活跃 IOC 的标准"
            )
            return -1
        # 如果以上两个条件都满足，则认为 IOC 是活跃的
        return 1

    def get_pulses_active_iocs(self, pulse_id: str, timeout: int = 30) -> dict:
        """获取指定 pulse 的活跃 IOC
        活跃判断: is_active 为 True 且 created 在 1 年内

        Args:
            pulse_id (str): Pulse 的 ID
            timeout (int, optional): 请求超时时间,单位为秒(默认30秒
        Returns:
            dict: 包含活跃 IOC 的字典，包含 count 和 indicators 字段
        """
        active_iocs = []
        result = {}

        response_json = self.get_pulses_indicators_by_page(
            pulse_id=pulse_id,
            page=1,  # 默认第一页
            limit=100,  # 每页100条
            timeout=timeout,
        )
        iocs_100 = response_json.get("results", [])
        active_iocs_100 = [
            ioc for ioc in iocs_100 if self.validate_ioc(ioc_info=ioc) == 1
        ]
        active_iocs.extend(active_iocs_100)

        # 当前限制分页大小为100条，response_json 中的 count 字段如果大于100，则需要继续获取
        total_count = response_json.get("count", 0)
        result["count"] = total_count

        # 上一次请求结束的时间
        last_request_time = datetime.now()

        if total_count > 100:
            OTX_API_LOGGER.info(
                f"Pulse {pulse_id} 的 Indicators 数量超过100, 需要进行分页查询"
            )
            # 分页查询 ioc 并判断是否活跃, 直到时间大于 1 年停止查询
            for page in range(2, (total_count // 100) + 2):
                OTX_API_LOGGER.info(
                    f"正在获取 Pulse {pulse_id} 的 Indicators 第 {page} 页"
                )
                page_response = self.get_pulses_indicators_by_page(
                    pulse_id=pulse_id,
                    page=page,
                    limit=100,
                    timeout=timeout,
                )
                iocs_page = page_response.get("results", [])
                active_iocs_page = [
                    ioc for ioc in iocs_page if self.validate_ioc(ioc_info=ioc) == 1
                ]
                active_iocs.extend(active_iocs_page)
                # 如果当前页的结果中没有活跃 IOC，则停止查询
                if not active_iocs_page:
                    OTX_API_LOGGER.info(
                        f"Pulse {pulse_id} 的 Indicators 第 {page} 页没有活跃 IOC, 停止查询"
                    )
                    break

                current_time = datetime.now()
                # 如果当前时间距离上一次查询已经过去了 3 s 那么继续下一次查询, 如果没有 3s 那么等待到 3s 后再进行下一次查询
                if (current_time - last_request_time).total_seconds() < 3:
                    wait_time = 3 - (current_time - last_request_time).total_seconds()
                    OTX_API_LOGGER.info(f"等待 {wait_time:.2f} 秒后继续查询下一页")
                    time.sleep(wait_time)

                last_request_time = datetime.now()

        result["indicators"] = active_iocs
        OTX_API_LOGGER.info(
            f"Pulse {pulse_id} 的 Indicators 共查询到 {len(active_iocs)} 条活跃 IOC"
        )

        return result
