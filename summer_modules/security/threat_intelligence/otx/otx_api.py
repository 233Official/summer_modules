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
        # 注册退出时保存数据
        atexit.register(self.save_data)

    def init_pulses_base_info(self):
        """
        初始化OTX API的Pulses基本信息
        :return: None
        """
        # 读取最近修改的5000个Pulses的基本信息
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
        write_dict_to_json_file(
            self.pulses_subscriber_count_desc_sorted_id_list,
            PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH,
            one_line=True,
        )
        write_list_to_txt_file(
            self.pulses_subscriber_count_desc_sorted_id_list,
            PULSES_SUBSCRIBER_COUNT_DESC_SORTED_ID_LIST_FILEPATH,
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
                '排序字段无效, 可选项有 "-modified", "modified", "-created", "created"'
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

        with httpx.Client() as client:
            # 最多重试3次
            headers = {"X-OTX-API-KEY": self.otx_api_key}
            response = client.get(
                SEARCH_PULSE_URL, headers=headers, params=params, timeout=timeout
            )
            for _ in range(2):
                if response.status_code == 200:
                    break
                else:
                    OTX_API_LOGGER.error(
                        f"请求失败, 状态码: {response.status_code}, 重试中..."
                    )
                    response = client.get(
                        SEARCH_PULSE_URL,
                        headers=self.headers,
                        params=params,
                        timeout=timeout,
                    )
            else:
                # 如果重试3次仍然失败,则抛出异常
                raise Exception(f"尝试请求3次依旧失败, 状态码: {response.status_code}")

            response.raise_for_status()
            response_json = response.json()
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
