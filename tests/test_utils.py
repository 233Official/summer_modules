from pathlib import Path
from tests import SUMMER_MODULES_TEST_LOGGER
from summer_modules.utils import (
    get_all_json_files,
    read_json_file_to_dict,
    write_dict_to_json_file,
    retry,
)
import asyncio

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


def main():
    fetch_data("http://example.com")
    asyncio.run(fetch_data_async("http://example.com"))


if __name__ == "__main__":
    main()
