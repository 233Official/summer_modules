import pytest
import json
import zlib
from pathlib import Path
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
import logging

from summer_modules.database.hbase.hbase_api import HBaseAPI
from summer_modules.database.hbase.hbase.ttypes import (
    ColumnDescriptor,
    Mutation,
    TScan,
    TColumn,
    TRowResult,
    TCell,
)
from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG

# 从配置文件获取连接信息
HBASE_HOST = CONFIG["hbase"]["host"]
HBASE_PORT = CONFIG["hbase"]["port"]
HBASE_USERNAME = CONFIG["hbase"]["username"]
HBASE_PASSWORD = CONFIG["hbase"]["password"]

SUMMER_MODULES_TEST_LOGGER.setLevel(logging.INFO)


def run_safe_tests():
    """运行安全的只读测试"""
    # 记录测试开始
    SUMMER_MODULES_TEST_LOGGER.info("=" * 50)
    SUMMER_MODULES_TEST_LOGGER.info(
        f"开始安全的 HBase API 测试 - 连接到 {HBASE_HOST}:{HBASE_PORT}"
    )
    SUMMER_MODULES_TEST_LOGGER.info("=" * 50)

    # 初始化 API
    try:
        hbase = HBaseAPI(
            host=HBASE_HOST,
            port=HBASE_PORT,
            username=HBASE_USERNAME,
            password=HBASE_PASSWORD,
        )
        SUMMER_MODULES_TEST_LOGGER.info("✅ HBase 连接成功")
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ HBase 连接失败: {e}")
        return False

    test_results = {
        "连接测试": True,
        "获取表列表": None,
        "表存在性检查": None,
        "获取单行数据": None,
        "获取行范围数据": None,
        "获取带列过滤的数据": None,
        "数据解析测试": None,
    }

    # 测试 1: 获取表列表
    try:
        tables = hbase._client.getTableNames()
        table_names = [t.decode() for t in tables]
        SUMMER_MODULES_TEST_LOGGER.info(
            f"✅ 获取到 {len(table_names)} 个表: {', '.join(table_names[:5])}..."
        )
        test_results["获取表列表"] = True

        # 选择一个表进行测试 (优先选择名称包含test的表)
        test_table = next(
            (t for t in table_names if "test" in t.lower()), table_names[0]
        )
        SUMMER_MODULES_TEST_LOGGER.info(f"选择表 '{test_table}' 进行后续测试")
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 获取表列表失败: {e}")
        test_results["获取表列表"] = False
        return test_results

    # 测试 2: 表存在性检查
    try:
        exists = hbase.table_exists(test_table)
        SUMMER_MODULES_TEST_LOGGER.info(f"✅ 表 '{test_table}' 存在性检查: {exists}")
        test_results["表存在性检查"] = True
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 表存在性检查失败: {e}")
        test_results["表存在性检查"] = False

    # 测试 3: 获取一些数据以找到有效的行键
    try:
        # 获取最多10行数据用于后续测试
        rows = hbase.get_rows(test_table, "", "zzzzzzzzz", include_timestamp=False)
        if rows:
            row_keys = [row["row_key"] for row in rows]
            SUMMER_MODULES_TEST_LOGGER.info(
                f"✅ 获取到 {len(rows)} 行数据，行键: {row_keys[:3]}..."
            )
            test_results["获取行范围数据"] = True

            # 从获取的数据中选择一个行键用于单行测试
            test_row_key = row_keys[0]

            # 测试 4: 获取单行数据
            try:
                row = hbase.get_row(test_table, test_row_key)
                if row:
                    # 获取第一个列族和列
                    first_cf = next(iter(row.keys() - {"row_key"}))
                    column_count = len(row[first_cf])
                    SUMMER_MODULES_TEST_LOGGER.info(
                        f"✅ 获取单行数据成功: 行键={test_row_key}, 列族={first_cf}, 列数={column_count}"
                    )
                    test_results["获取单行数据"] = True

                    # 记录列名，用于后续测试
                    all_columns = []
                    for cf in row.keys():
                        if cf != "row_key":
                            for col in row[cf].keys():
                                all_columns.append(f"{cf}:{col}")

                    # 测试 5: 获取带列过滤的数据
                    if all_columns:
                        try:
                            test_columns = all_columns[:2]  # 使用前两列进行测试
                            row_with_cols = hbase.get_row_with_columns(
                                test_table, test_row_key, test_columns
                            )
                            if row_with_cols:
                                SUMMER_MODULES_TEST_LOGGER.info(
                                    f"✅ 获取带列过滤的数据成功: 行键={test_row_key}, 列={test_columns}"
                                )
                                test_results["获取带列过滤的数据"] = True
                            else:
                                SUMMER_MODULES_TEST_LOGGER.warning(
                                    f"⚠️ 获取带列过滤的数据返回空: 行键={test_row_key}, 列={test_columns}"
                                )
                                test_results["获取带列过滤的数据"] = False
                        except Exception as e:
                            SUMMER_MODULES_TEST_LOGGER.error(
                                f"❌ 获取带列过滤的数据失败: {e}"
                            )
                            test_results["获取带列过滤的数据"] = False
                else:
                    SUMMER_MODULES_TEST_LOGGER.warning(f"⚠️ 行键 {test_row_key} 不存在")
                    test_results["获取单行数据"] = False
            except Exception as e:
                SUMMER_MODULES_TEST_LOGGER.error(f"❌ 获取单行数据失败: {e}")
                test_results["获取单行数据"] = False

            # 测试 6: 数据解析测试
            try:
                # 模拟测试压缩数据的解析逻辑
                test_data = {"compressed": "value", "test": True}
                compressed_data = zlib.compress(json.dumps(test_data).encode())

                # 测试解压缩和解析逻辑 (不需要访问数据库)
                try:
                    # 直接测试代码中的解析逻辑
                    value = None
                    try:
                        # 先尝试解码为 UTF-8 (这肯定会失败，因为是压缩数据)
                        value_str = compressed_data.decode()
                    except UnicodeDecodeError:
                        # 尝试解压
                        try:
                            decompressed = zlib.decompress(compressed_data)
                            value = json.loads(decompressed.decode())
                        except Exception as inner_e:
                            SUMMER_MODULES_TEST_LOGGER.error(
                                f"解压或解析失败: {inner_e}"
                            )

                    if value == test_data:
                        SUMMER_MODULES_TEST_LOGGER.info("✅ 压缩数据解析测试成功")
                        test_results["数据解析测试"] = True
                    else:
                        SUMMER_MODULES_TEST_LOGGER.warning(
                            f"⚠️ 压缩数据解析结果与预期不符: {value} != {test_data}"
                        )
                        test_results["数据解析测试"] = False
                except Exception as parse_e:
                    SUMMER_MODULES_TEST_LOGGER.error(
                        f"❌ 压缩数据解析测试失败: {parse_e}"
                    )
                    test_results["数据解析测试"] = False
            except Exception as e:
                SUMMER_MODULES_TEST_LOGGER.error(f"❌ 数据解析测试失败: {e}")
                test_results["数据解析测试"] = False
        else:
            SUMMER_MODULES_TEST_LOGGER.warning(f"⚠️ 表 {test_table} 没有数据")
            test_results["获取行范围数据"] = False
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 获取行范围数据失败: {e}")
        test_results["获取行范围数据"] = False

    # 关闭连接
    try:
        hbase.close()
        SUMMER_MODULES_TEST_LOGGER.info("✅ HBase 连接已关闭")
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ HBase 连接关闭失败: {e}")

    # 打印测试摘要
    SUMMER_MODULES_TEST_LOGGER.info("\n" + "=" * 50)
    SUMMER_MODULES_TEST_LOGGER.info("HBase API 测试摘要:")
    for test_name, result in test_results.items():
        status = "✅ 通过" if result else "❌ 失败" if result is False else "⚠️ 未测试"
        SUMMER_MODULES_TEST_LOGGER.info(f"{test_name}: {status}")
    SUMMER_MODULES_TEST_LOGGER.info("=" * 50)

    return test_results


def test_count_rows_with_timerage_via_ssh():
    """测试通过 SSH 获取指定时间范围的数据的功能"""
    hbase = HBaseAPI(
        host=HBASE_HOST,
        port=HBASE_PORT,
        username=HBASE_USERNAME,
        password=HBASE_PASSWORD,
    )

    # 开始时间为北京时间 2025.6.19 12:00:00
    start_datetime = datetime(2025, 6, 19, 12, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.20 00:00:00
    # end_datetime = start_datetime + timedelta(days=1)
    # 结束时间为北京时间 2025.6.19 18:00:00
    end_datetime = datetime(2025, 6, 19, 18, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    result = hbase.count_rows_with_timerage_via_ssh(
        table_name="cloud-whoisxml-whois-data",
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )
    SUMMER_MODULES_TEST_LOGGER.info(
        f"表 'cloud-whoisxml-whois-data' 在时间范围 [{start_datetime}, {end_datetime}] 内的行数: {result}"
    )


def test_get_data_with_timerage_via_ssh():
    """测试通过 SSH 获取指定时间范围的数据的功能"""
    start_time = time.time()
    SUMMER_MODULES_TEST_LOGGER.info("开始测试获取数据功能")
    hbase = HBaseAPI(
        host=HBASE_HOST,
        port=HBASE_PORT,
        username=HBASE_USERNAME,
        password=HBASE_PASSWORD,
    )

    # 开始时间为北京时间 2025.6.19 12:00:00
    # start_datetime = datetime(2025, 6, 19, 12, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.20 00:00:00
    # end_datetime = start_datetime + timedelta(days=1)
    # 结束时间为北京时间 2025.6.19 18:00:00
    # end_datetime = datetime(2025, 6, 19, 18, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:00:00
    # end_datetime = datetime(2025, 6, 19, 15, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 开始时间为北京时间 2025.6.19 15:00:00
    start_datetime = datetime(2025, 6, 19, 15, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 结束时间为北京时间 2025.6.19 16:30:00 (182257 rows) Took 220.7019 seconds
    end_datetime = datetime(2025, 6, 19, 16, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:45:00
    # end_datetime = datetime(2025, 6, 19, 15, 45, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:22:30
    # end_datetime = datetime(2025, 6, 19, 15, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 开始时间为北京时间 2025.6.19 15:22:30
    # start_datetime = datetime(2025, 6, 19, 15, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:33:45
    # end_datetime = datetime(2025, 6, 19, 15, 33, 45, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 开始时间为北京时间 2025.6.19 15:33:45
    # start_datetime = datetime(2025, 6, 19, 15, 33, 45, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:40:00
    # end_datetime = datetime(2025, 6, 19, 15, 40, 00, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:37:00
    # end_datetime = datetime(2025, 6, 19, 15, 37, 00, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 开始时间为北京时间 2025.6.19 15:37:00
    # start_datetime = datetime(2025, 6, 19, 15, 37, 00, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:40:00
    # end_datetime = datetime(2025, 6, 19, 15, 40, 00, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:38:39
    # end_datetime = datetime(2025, 6, 19, 15, 38, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    # 结束时间为北京时间 2025.6.19 15:37:45 (12098 rows)
    # end_datetime = datetime(2025, 6, 19, 15, 37, 45, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 测试全量获取数据
    # result = hbase.get_data_with_timerage_via_ssh(
    #     table_name="cloud-whoisxml-whois-data",
    #     start_datetime=start_datetime,
    #     end_datetime=end_datetime,
    # )
    # 测试获取 3 条数据
    # result = hbase.get_data_with_timerage_via_ssh(
    #     table_name="cloud-whoisxml-whois-data",
    #     start_datetime=start_datetime,
    #     end_datetime=end_datetime,
    #     batch_size=3,
    # )
    # 测试从指定起始行键开始获取 3 条数据
    # result = hbase.get_data_with_timerage_via_ssh(
    #     table_name="cloud-whoisxml-whois-data",
    #     start_datetime=start_datetime,
    #     end_datetime=end_datetime,
    #     batch_size=3,
    #     start_row_key="0.qidianmoji.cn-9223370286649119042",
    # )

    # 测试分批全量获取数据
    # result = hbase.get_data_with_timerage_batches_via_ssh(
    #     table_name="cloud-whoisxml-whois-data",
    #     start_datetime=start_datetime,
    #     end_datetime=end_datetime,
    # )

    # 测试分批全量获取数据(包含 max_limit)
    result = hbase.get_data_with_timerage_batches_via_ssh(
        table_name="cloud-whoisxml-whois-data",
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        # max_limit=100000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        # max_limit=10000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        max_limit=3,  # 设置最大行数限制，避免过多数据导致程序崩溃
    )

    if result.success:
        SUMMER_MODULES_TEST_LOGGER.info(
            f"成功获取 {len(result.rows)} 行数据，表名: {result.table_name}"
        )
        for row in result.rows:
            SUMMER_MODULES_TEST_LOGGER.info(
                f"行键: {row.row_key}, 列数: {len(row.columns)}"
            )
    else:
        SUMMER_MODULES_TEST_LOGGER.error(f"获取数据失败: {result.error_message}")

    # 测试分批全量获取数据(包含 max_limit)
    result = hbase.get_data_with_timerage_batches_via_ssh(
        table_name="cloud-whoisxml-whois-data",
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        # max_limit=100000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        # max_limit=10000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        start_row_key="0.time.chameleoncloud.co.uk-9223370286645797690",
        max_limit=3,  # 设置最大行数限制，避免过多数据导致程序崩溃
    )

    if result.success:
        SUMMER_MODULES_TEST_LOGGER.info(
            f"成功获取 {len(result.rows)} 行数据，表名: {result.table_name}"
        )
        for row in result.rows:
            SUMMER_MODULES_TEST_LOGGER.info(
                f"行键: {row.row_key}, 列数: {len(row.columns)}"
            )
    else:
        SUMMER_MODULES_TEST_LOGGER.error(f"获取数据失败: {result.error_message}")

    SUMMER_MODULES_TEST_LOGGER.info("获取数据测试完成")
    end_time = time.time()
    execution_time = end_time - start_time
    SUMMER_MODULES_TEST_LOGGER.info(f"获取数据测试总耗时: {execution_time:.2f} 秒")


# 测试批量获取的改进方案
def test_get_data_with_timerage_batches_via_ssh_imporve():
    """测试通过 SSH 分批获取指定时间范围的数据的功能"""

    start_time = time.time()
    SUMMER_MODULES_TEST_LOGGER.info("开始测试获取数据功能")
    hbase = HBaseAPI(
        host=HBASE_HOST,
        port=HBASE_PORT,
        username=HBASE_USERNAME,
        password=HBASE_PASSWORD,
    )

    # 开始时间为北京时间 2025.6.19 15:00:00
    start_datetime = datetime(2025, 6, 19, 15, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 结束时间为北京时间 2025.6.19 16:30:00 (182257 rows) Took 220.7019 seconds
    end_datetime = datetime(2025, 6, 19, 16, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 测试分批全量获取数据(包含 max_limit)
    result1 = hbase.get_data_with_timerage_batches_via_ssh(
        table_name="cloud-whoisxml-whois-data",
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        # max_limit=100000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        # max_limit=10000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        max_limit=3,  # 设置最大行数限制，避免过多数据导致程序崩溃
    )

    if result1.success:
        SUMMER_MODULES_TEST_LOGGER.info(
            f"成功获取 {len(result1.rows)} 行数据，表名: {result1.table_name}"
        )
    else:
        SUMMER_MODULES_TEST_LOGGER.error(f"获取数据失败: {result1.error_message}")

    # 测试分批全量获取数据(包含 max_limit)
    result2 = hbase.get_data_with_timerage_batches_via_ssh(
        table_name="cloud-whoisxml-whois-data",
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        # max_limit=100000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        # max_limit=10000,  # 设置最大行数限制，避免过多数据导致程序崩溃
        # start_row_key="0.time.chameleoncloud.co.uk-9223370286645797690",
        start_row_key= result1.last_row_key,  # 使用上一次获取的最后一行键作为起始行键
        max_limit=3,  # 设置最大行数限制，避免过多数据导致程序崩溃
    )

    if result2.success:
        SUMMER_MODULES_TEST_LOGGER.info(
            f"成功获取 {len(result2.rows)} 行数据，表名: {result2.table_name}"
        )
    else:
        SUMMER_MODULES_TEST_LOGGER.error(f"获取数据失败: {result2.error_message}")

    SUMMER_MODULES_TEST_LOGGER.info("两次获取的所有数据的行键:")
    for row in result1.rows + result2.rows:
        SUMMER_MODULES_TEST_LOGGER.info(f"{row.row_key}")

    SUMMER_MODULES_TEST_LOGGER.info(
        f"第一次获取数据返回的 last_row_key: {result1.last_row_key}, 第二次获取数据返回的 last_row_key: {result2.last_row_key}"
    )


def test_reverse_timestamp_to_normal():
    """测试将时间戳转换为正常时间"""
    hbase = HBaseAPI(
        host=HBASE_HOST,
        port=HBASE_PORT,
        username=HBASE_USERNAME,
        password=HBASE_PASSWORD,
    )

    # 测试时间戳
    timestamp = 9223370286617113050
    normal_time = hbase.reverse_timestamp_to_normal(timestamp)
    SUMMER_MODULES_TEST_LOGGER.info(
        f"时间戳 {timestamp} 转换为正常时间: {normal_time}"
    )

if __name__ == "__main__":
    # run_safe_tests()
    # test_count_rows_with_timerage_via_ssh()
    # test_get_data_with_timerage_via_ssh()
    # test_get_data_with_timerage_batches_via_ssh_imporve()
    test_reverse_timestamp_to_normal()