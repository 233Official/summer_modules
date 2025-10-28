import toml
from openai import OpenAI
from pathlib import Path
from summer_modules.logger import init_and_get_logger

CURRENT_DIR = Path(__file__).resolve().parent
logger = init_and_get_logger(CURRENT_DIR, "deepseek_loogger")


class DeepseekClient:
    """DeepseekClient 类封装了与 Deepseek API 交互的功能"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        self.client.temperature = 1.3  # type: ignore

    def translate_text(self, text: str) -> str | None:
        """
        使用 DeepSeek API 将英文翻译成中文
        """
        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {
                        "role": "system",
                        "content": "你是一个专业的翻译助手，请将用户输入的英文翻译成中文。",
                    },
                    {"role": "user", "content": f"请将下面的英文翻译成中文：\n{text}"},
                ],
                temperature=0.3,
            )
            content = response.choices[0].message.content
            logger.info(f"\n原文: {text}\n译文: {content}\n")
            return content
        except Exception as e:
            logger.error(f"翻译出错: {str(e)}")
            return f"翻译出错: {str(e)}"
