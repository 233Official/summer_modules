import toml
from openai import OpenAI
from pathlib import Path
from summer_modules.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_TOML_FILEPATH = (CURRENT_DIR / "../../config.toml").resolve()
CONFIG = toml.load(CONFIG_TOML_FILEPATH)
DEEPSEEK_APIKEY = CONFIG["deepseek_apikey"]
logger = init_and_get_logger(CURRENT_DIR)


# 配置 DeepSeek API
client = OpenAI(
    api_key=DEEPSEEK_APIKEY,
    base_url="https://api.deepseek.com"
)
client.temperature = 1.3

def translate_text(text: str) -> str:
    """
    使用 DeepSeek API 将英文翻译成中文
    """
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": "你是一个专业的翻译助手，请将用户输入的英文翻译成中文。"
                },
                {
                    "role": "user",
                    "content": f"请将下面的英文翻译成中文：\n{text}"
                }
            ],
            temperature=0.3
        )
        content = response.choices[0].message.content
        logger.info(f"\n原文: {text}\n译文: {content}\n")
        return content
    except Exception as e:
        logger.error(f"翻译出错: {str(e)}")
        return f"翻译出错: {str(e)}"