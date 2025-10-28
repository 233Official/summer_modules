from pathlib import Path
import toml

from tests import CONFIG

CURRENT_DIR = Path(__file__).parent.resolve()

IMAGE_HOST_CONFIG = CONFIG.get("image_host", {})
if not IMAGE_HOST_CONFIG:
    raise ValueError("配置文件中未找到 image_host 配置项")
