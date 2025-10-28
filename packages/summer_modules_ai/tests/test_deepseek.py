import types

import pytest

from summer_modules_ai import DeepseekClient


class DummyMessage:
    def __init__(self, content: str):
        self.content = content


class DummyChoice:
    def __init__(self, content: str):
        self.message = DummyMessage(content)


class DummyResponse:
    def __init__(self, content: str):
        self.choices = [DummyChoice(content)]


class DummyCompletions:
    def __init__(self, response: DummyResponse):
        self._response = response
        self.last_kwargs = None

    def create(self, **kwargs):
        self.last_kwargs = kwargs
        return self._response


class DummyChat:
    def __init__(self, response: DummyResponse):
        self.completions = DummyCompletions(response)


class DummyClient:
    def __init__(self, response: DummyResponse):
        self.chat = DummyChat(response)


def test_translate_text_success(monkeypatch):
    dummy_response = DummyResponse("翻译后的文本")

    client = DummyClient(dummy_response)
    deepseek = DeepseekClient(api_key="dummy", client=client)  # type: ignore[arg-type]

    result = deepseek.translate_text("Hello")

    assert result == "翻译后的文本"
    kwargs = client.chat.completions.last_kwargs
    assert kwargs is not None
    assert kwargs["model"] == "deepseek-chat"
    assert kwargs["messages"][1]["content"].startswith("请将下面的英文翻译成中文")


def test_translate_text_failure():
    def fail(**_):
        raise RuntimeError("boom")

    failing_client = types.SimpleNamespace(
        chat=types.SimpleNamespace(
            completions=types.SimpleNamespace(create=fail)
        )
    )

    deepseek = DeepseekClient(api_key="dummy", client=failing_client)  # type: ignore[arg-type]

    result = deepseek.translate_text("Hello")

    assert result == "翻译出错: boom"
