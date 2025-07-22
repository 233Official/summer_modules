from pathlib import Path
from tests import SUMMER_MODULES_TEST_LOGGER
from summer_modules.utils import (
    get_all_json_files,
    read_json_file_to_dict,
    write_dict_to_json_file,
    retry,
    convert_timestamp_to_timezone_time,
    convert_timezone_time_to_timezone_time,
    convert_timezone_time_to_utc,
)
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

CURRENT_DIR = Path(__file__).resolve().parent


def test_get_all_json_files():
    get_all_json_files(CURRENT_DIR)
    SUMMER_MODULES_TEST_LOGGER.info(
        f"黑域名文件数量: {len(get_all_json_files(CURRENT_DIR))}"
    )


# 同步函数示例
@retry(max_retries=3, delay=1.0, backoff_strategy="exponential")
def fetch_data(url):
    # 同步操作...
    SUMMER_MODULES_TEST_LOGGER.info(f"Fetching data from {url}")
    # 模拟可能的异常
    if url == "http://example.com":
        raise ValueError("Simulated error for testing")
    pass


# 异步函数示例
@retry(max_retries=3, delay=1.0, backoff_strategy="exponential")
async def fetch_data_async(url):
    # 异步操作...
    SUMMER_MODULES_TEST_LOGGER.info(f"Fetching data asynchronously from {url}")
    await asyncio.sleep(1)  # 模拟异步操作
    # 模拟可能的异常
    if url == "http://example.com":
        raise ValueError("Simulated error for testing")
    pass


def test_fetch_data():
    fetch_data("http://example.com")
    asyncio.run(fetch_data_async("http://example.com"))


def test_convert_timezone_time_to_timezone_time():
    current_time = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
    tokyo_time = convert_timezone_time_to_timezone_time(
        time=current_time, to_zone=ZoneInfo("Asia/Tokyo")
    )
    SUMMER_MODULES_TEST_LOGGER.info(
        f"当前时间: {current_time}, 转换后的东京时间: {tokyo_time}"
    )


def test_convert_timezone_time_to_utc():
    current_time = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
    utc_time = convert_timezone_time_to_utc(time=current_time)
    SUMMER_MODULES_TEST_LOGGER.info(
        f"当前时间: {current_time}, 转换后的UTC时间: {utc_time}"
    )


def main():
    # test_convert_timezone_time_to_timezone_time()
    test_convert_timezone_time_to_utc()


if __name__ == "__main__":
    main()
