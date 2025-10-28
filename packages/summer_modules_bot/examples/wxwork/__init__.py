"""Enterprise WeChat bot example utilities."""

from pathlib import Path
from typing import Any, Dict

from .. import load_example_config

CURRENT_DIR = Path(__file__).parent.resolve()


def get_wxwork_config() -> Dict[str, Any]:
    bot_config = load_example_config("bot")
    wxwork_config = bot_config.get("wxwork", {}) if isinstance(bot_config, dict) else {}
    return wxwork_config if isinstance(wxwork_config, dict) else {}
