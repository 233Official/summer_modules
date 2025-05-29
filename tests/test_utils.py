from pathlib import Path
from .test_main import SUMMER_MODULES_TEST_LOGGER
from summer_modules.utils import (
    get_all_json_files,
    read_json_file_to_dict,
    write_dict_to_json_file,
)

CURRENT_DIR = Path(__file__).resolve().parent


def test_get_all_json_files():
    get_all_json_files(CURRENT_DIR)
    SUMMER_MODULES_TEST_LOGGER.info(
        f"黑域名文件数量: {len(get_all_json_files(CURRENT_DIR))}"
    )
