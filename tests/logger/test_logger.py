"""
测试自定义 logger
"""

from tests import SUMMER_MODULES_TEST_LOGGER


# 测试 logger 的文件切分功能是否正常工作
def test_logger_file_split():
    # 输出超级多日志，测试文件切分
    log = "测试日志" * 1000
    for i in range(1000):
        SUMMER_MODULES_TEST_LOGGER.info(f"{i}-{log}")


def main():
    test_logger_file_split()


if __name__ == "__main__":
    main()
