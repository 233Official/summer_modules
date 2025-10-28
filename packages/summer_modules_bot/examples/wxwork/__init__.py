from pathlib import Path

from tests.bot import BOT_CONFIG

CURRENT_DIR = Path(__file__).parent.resolve()

WXWORKBOT_CONFIG = BOT_CONFIG["wxwork"]
WXWORKBOT_WEBHOOK = WXWORKBOT_CONFIG["webhook"]
