import httpx
import socket
from contextlib import contextmanager

@contextmanager
def custom_dns_mapping(mapping):
    """上下文管理器，临时修改 DNS 解析"""
    original_getaddrinfo = socket.getaddrinfo
    
    def custom_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        if host in mapping:
            host = mapping[host]
        return original_getaddrinfo(host, port, family, type, proto, flags)
    
    socket.getaddrinfo = custom_getaddrinfo
    try:
        yield
    finally:
        socket.getaddrinfo = original_getaddrinfo

# 使用示例
dns_mapping = {
    'summery.com': '127.0.0.1',
    'example.com': '192.168.1.100'
}

with custom_dns_mapping(dns_mapping):
    with httpx.Client() as client:
        response = client.get('http://summery.com:8000')
        print(f"Status: {response.status_code}, {response.text[:1000]}")
