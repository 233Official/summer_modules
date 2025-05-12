from pathlib import Path
import json
import random

CURRENT_DIR = Path(__file__).resolve().parent
USER_AGENT_FILEPATH = CURRENT_DIR / "browsers.json"

def getUserAgent():
    """读取 browsers.json 构造随机的 user-agent"""
    with open(USER_AGENT_FILEPATH, "r") as f:
        header = json.load(f)
    # print(type(header))
    browsers = header["browsers"]
    # print(f'{type(browsers)}')
    # 随机提取 browsers 中的一个键值对
    browser = random.choice(list(browsers.items()))
    return random.choice(browser[1])