from pathlib import Path
import toml
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from summer_modules.security.threat_intelligence.otx.otx_api import OTXApi
from summer_modules.utils import write_dict_to_json_file

from tests.test_main import SUMMER_MODULES_TEST_LOGGER


CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_TOML_FILEPATH = (CURRENT_DIR / "../../../../config.toml").resolve()
CONFIG_TOML = toml.load(CONFIG_TOML_FILEPATH)
OTX_API_KEY = CONFIG_TOML["otx_api_key"]
TMP_DIR = CURRENT_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

otx_api = OTXApi(otx_api_key=OTX_API_KEY)
pulses_id = "67f82020a26d2eb2bb6d4f1e"
# pulses_id = "67fb93e88bf6ed070ce7164a"
# pulses_info = otx_api.get_pulses_info(pulses_id)
current_timestamp = int(time.time())
# write_dict_to_json_file(
#     data=pulses_info,
#     filepath=TMP_DIR / f"{current_timestamp}_{pulses_id}_pulses_info.json",
#     one_line=False,
# )

pulses_indicators = otx_api.get_pulses_indicators(pulses_id)
SUMMER_MODULES_TEST_LOGGER.info(
    f"length of indicators: {len(pulses_indicators['indicators'])}"
)
write_dict_to_json_file(
    data=pulses_indicators,
    filepath=TMP_DIR
    / f"{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d%H%M%S')}_{pulses_id}_pulses_indicators.json",
    # one_line=True,
    one_line=False,
)

# # 看一个 ioc 超过 100 的 pulse
# pulse_id = "67e7f5af6e96759df2850fbe"
# iocs = otx_api.get_pulses_indicators(pulse_id)
# write_dict_to_json_file(
#     data=iocs,
#     filepath=TMP_DIR / f"{current_timestamp}_{pulse_id}_iocs.json",
#     one_line=True,
#     # one_line=False,
# )
# SUMMER_MODULES_TEST_LOGGER.info(f"length of iocs: {len(iocs['indicators'])}")

# SUMMER_MODULES_TEST_LOGGER.info(f"pulses_info: {pulses_info}")
