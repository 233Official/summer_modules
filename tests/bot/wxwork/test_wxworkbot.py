from summer_modules.bot.wxwork.wxworkbot import WXWorkBot
from summer_modules.markdown import Markdown

from tests.bot.wxwork import WXWORKBOT_WEBHOOK, CURRENT_DIR
from tests import SUMMER_MODULES_TEST_LOGGER
from tests.markdown.image_host import CURRENT_DIR as MARKDOWN_IMAGE_HOST_TESTS_DIR


wxworkbot = WXWorkBot(webhook=WXWORKBOT_WEBHOOK, logger=SUMMER_MODULES_TEST_LOGGER)


def test_post_md_message():
    wxworkbot.post_md_message("测试企微Bot推送markdown信息")
    wxworkbot.post_md_message(
        "测试企微Bot推送markdown信息，包含特殊字符：!@#$%^&*()_+{}|:\"<>?[];',./`~"
    )
    test_markdown = Markdown(markdown_file_path=CURRENT_DIR / "test_markdown.md")
    wxworkbot.post_md_message(test_markdown.content)


def test_post_md_message_v2_from_path():
    test_markdown_path = CURRENT_DIR / "test_markdown.md"
    wxworkbot.post_md_message_v2_from_path(test_markdown_path)


def test_post_img():
    img_path = MARKDOWN_IMAGE_HOST_TESTS_DIR / "laugh.jpg"
    wxworkbot.post_img_from_path(img_path=img_path)


def main():
    # test_post_md_message()
    # test_post_img()
    test_post_md_message_v2_from_path()


if __name__ == "__main__":
    main()
