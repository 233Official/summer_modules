import httpx
import traceback

from summer_modules.dns_resolver import get_httpx_mounts
from summer_modules import summer_modules_logger


def test_dns_resolver():
    dns_map = {
        "example.com": "127.0.0.1",
        "test.com": "127.0.0.1",
        "summery.com": "127.0.0.1",
    }
    mounts = get_httpx_mounts(dns_map)

    try:
        with httpx.Client(mounts=mounts) as client:
            response = client.get("http://summery.com:8000")
            summer_modules_logger.info(f"HTTP 响应状态码: {response.status_code}")
    except Exception as e:
        summer_modules_logger.error(f"请求失败: {e}, {traceback.format_exc()}")


def main():
    test_dns_resolver()


if __name__ == "__main__":
    main()
