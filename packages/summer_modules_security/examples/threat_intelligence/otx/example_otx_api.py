"""Demo script to download OTX indicators for a pulse."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from summer_modules_core import load_config
from summer_modules_core.logger import init_and_get_logger
from summer_modules_core.utils import write_dict_to_json_file

from summer_modules_security import get_storage_dir
from summer_modules_security.threat_intelligence.otx.otx_api import OTXApi

EXAMPLE_PULSE_ID = "682410120b78adf5bf8753dc"
OUTPUT_DIR = get_storage_dir("examples", "otx")
LOGGER = init_and_get_logger(OUTPUT_DIR, "otx_example")


def main() -> None:
    config = load_config("security")
    api_key = config.get("otx_api_key")
    if not api_key:
        raise RuntimeError("请在 config.toml 的 [security] 中配置 otx_api_key。")

    api = OTXApi(api_key, data_dir=get_storage_dir("threat_intelligence", "otx_examples"))
    indicators = api.get_pulses_indicators(EXAMPLE_PULSE_ID)
    LOGGER.info("获取到 %s 条 IOC 数据", len(indicators.get("results", [])))

    timestamp = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
    output_path = OUTPUT_DIR / f"{timestamp}_{EXAMPLE_PULSE_ID}_indicators.json"
    write_dict_to_json_file(indicators, output_path, one_line=False)
    LOGGER.info("已将示例数据写入 %s", output_path)


if __name__ == "__main__":
    main()
