from pathlib import Path
import toml

from tests import CONFIG

CURRENT_DIR = Path(__file__).parent.resolve()

OSS_CONFIG = CONFIG["oss"]
if not OSS_CONFIG:
    raise ValueError("配置文件中未找到 oss 配置项")
