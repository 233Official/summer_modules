from httpx import HTTPTransport
import httpx


def get_httpx_mounts(dns_mapping: dict[str, str]) -> dict:
    """
    为 httpx 客户端创建带有自定义 DNS 解析的 mounts 配置

    Args:
        dns_mapping: 域名到IP地址的映射字典

    Returns:
        配置好的 mounts 字典，可直接用于 httpx.Client
    """
    mounts = {}
    for domain, ip in dns_mapping.items():
        # 正确格式：domain://
        url_pattern = f"{domain}://"

        # 使用 connect_to 参数直接指定连接目标
        # 这才是正确的DNS覆盖方式
        transport = httpx.HTTPTransport(
            # 将对该域名的所有连接重定向到指定IP
            connect_to={(domain, 80): (ip, 80), (domain, 443): (ip, 443)}  # type: ignore
        )
        mounts[url_pattern] = transport

    return mounts
