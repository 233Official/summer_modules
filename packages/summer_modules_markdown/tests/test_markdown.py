import re
from pathlib import Path

from summer_modules_markdown import Markdown


def test_markdown_build_and_save(tmp_path: Path) -> None:
    md_path = tmp_path / "doc.md"
    markdown = Markdown(markdown_file_path=md_path)

    markdown.add_full_title("主标题")
    markdown.add_header("二级标题", level=2)
    markdown.add_paragraph("第一段")
    markdown.add_horizontal_rule()
    markdown.add_code_block("print('hello')", language="python")
    markdown.add_list(["项目一", "项目二"])
    markdown.add_table(["列1", "列2"], [["A", "B"]])
    markdown.add_local_image(Path("/tmp/image.png"), alt_text="示例图")

    markdown.save()
    content = md_path.read_text(encoding="utf-8")

    assert "# 主标题" in content
    assert "## 二级标题" in content
    assert "第一段" in content
    assert "```python" in content
    assert re.search(r"\| 列1 \| 列2 \|", content)
    assert "![示例图](/tmp/image.png)" in content


def test_markdown_clear_all(tmp_path: Path) -> None:
    md_path = tmp_path / "doc.md"
    md_path.write_text("内容", encoding="utf-8")
    markdown = Markdown(markdown_file_path=md_path)

    markdown.clear_all()
    assert not md_path.exists()
    assert markdown.content == ""
