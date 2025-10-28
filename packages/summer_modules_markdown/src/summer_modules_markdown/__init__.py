# markdown 模块,用于基本的 markdown 元素添加
from pathlib import Path
from typing import Union

from summer_modules_core.logger import init_and_get_logger

PACKAGE_ROOT = Path(__file__).parent.resolve()
MARKDOWN_LOGGER = init_and_get_logger(
    current_dir=PACKAGE_ROOT, logger_name="markdown_logger"
)
TMP_MARKDOWN_FILEPATH = (PACKAGE_ROOT / "tmp_markdown.md").resolve()

__all__ = ["Markdown", "MARKDOWN_LOGGER", "TMP_MARKDOWN_FILEPATH"]


class Markdown:
    def __init__(self, markdown_file_path: Path = TMP_MARKDOWN_FILEPATH) -> None:
        self.markdown_file_path = markdown_file_path
        """markdown 文件路径"""
        MARKDOWN_LOGGER.info(f"Markdown file path: {self.markdown_file_path}")
        self.content = ""
        """markdown 文件内容"""
        # 如果 markdown_file_path 是临时文件路径, 则在初始化时清空内容
        if self.markdown_file_path == TMP_MARKDOWN_FILEPATH:
            MARKDOWN_LOGGER.info("使用临时 Markdown 文件路径, 初始化时清空内容")
            self.clear_all()
        self.load()
        MARKDOWN_LOGGER.info("Markdown module initialized.")

    def load(self) -> None:
        """加载 Markdown 文件内容"""
        MARKDOWN_LOGGER.info(f"加载 markdown 文件内容: {self.markdown_file_path}")
        if not self.markdown_file_path.exists():
            MARKDOWN_LOGGER.warning(
                f"Markdown 文件不存在: {self.markdown_file_path}, 无需加载"
            )
            return

        with open(self.markdown_file_path, "r", encoding="utf-8") as f:
            self.content = f.read()
        MARKDOWN_LOGGER.info("已加载 Markdown 文件内容。")

    def save(self) -> None:
        """保存 Markdown 文件"""
        MARKDOWN_LOGGER.info(f"保存 markdown 内容到 {self.markdown_file_path}")
        with open(self.markdown_file_path, "w", encoding="utf-8") as f:
            f.write(self.content)
        MARKDOWN_LOGGER.info("已保存 Markdown 文件。")

    def clear_all(self) -> None:
        """清空 Markdown 文件内容以及本地 markdown 文件(如果存在)内容"""
        MARKDOWN_LOGGER.info("清空 Markdown 文件内容和本地文件内容")
        self.content = ""
        if self.markdown_file_path.exists():
            self.markdown_file_path.unlink()
            MARKDOWN_LOGGER.info(f"已删除本地 Markdown 文件: {self.markdown_file_path}")
        else:
            MARKDOWN_LOGGER.warning(
                f"本地 Markdown 文件不存在: {self.markdown_file_path}"
            )

    def add_full_title(self, title: str) -> None:
        """在 markdown 内容最前添加全文标题

        Args:
            title (str): 标题内容
        """
        self.content = f"# {title}\n\n" + self.content

    def add_header(self, header: str, level: int = 1) -> None:
        """添加标题到 Markdown 文件

        Args:
            header (str): 标题内容
            level (int): 标题级别, 1-6, 默认1
        """
        self.content += f"{'#' * level} {header}\n\n"

    def add_paragraph(
        self, paragraph: str, indent: int = 0, sub_level: int = 0
    ) -> None:
        """添加段落到 Markdown 文件

        Args:
            paragraph (str): 段落内容
            indent (int): 缩进级别, 默认0
            sub_level (int): 子级别, 用于处理多级标题, 默认0; 例如 sub_level=1 时, 所有的 # 标题会增加一级
        """
        # 处理段落内部的每一行，保持缩进一致
        indentation = "  " * indent

        # 按行分割段落
        paragraph_lines = paragraph.split("\n")

        # 对每行应用缩进
        indented_paragraph = "\n".join(
            f"{indentation}{line}" for line in paragraph_lines
        )

        # 处理标题级别
        if sub_level > 0:
            # 如果有子级别, 则增加标题级别(匹配 `# ` 更改为 `## `)
            indented_paragraph = indented_paragraph.replace(
                "# ", "#" * (sub_level + 1) + " "
            )

        # 添加到内容中，并确保段落后有两个换行
        self.content += f"{indented_paragraph}\n\n"

    # 添加分隔符
    def add_horizontal_rule(self) -> None:
        """添加水平分隔线到 Markdown 文件"""
        self.content += "\n\n---\n\n"

    def add_code_block(
        self, code: str, language: str = "python", indent: int = 0
    ) -> None:
        """添加代码块到 Markdown 文件

        Args:
            code (str): 代码内容
            language (str): 代码语言, 默认python
            indent (int): 缩进级别, 默认0
        """
        # 处理代码内部的每一行，保持缩进一致
        indentation = "  " * indent

        # 按行分割代码
        code_lines = code.split("\n")

        # 对每行应用缩进
        indented_code = "\n".join(f"{indentation}{line}" for line in code_lines)

        # 组装最终的代码块
        self.content += (
            f"{indentation}```{language}\n{indented_code}\n{indentation}```\n\n"
        )

    def add_list(self, items: list, ordered: bool = False, indent: int = 0) -> None:
        """添加列表到 Markdown 文件

        Args:
            items (list): 列表内容
            ordered (bool): 是否为有序列表, 默认False
            indent (int): 缩进级别, 默认0
        """
        self.content += (
            "\n".join(
                f"{'  ' * indent}{'1.' if ordered else '-'} {item}" for item in items
            )
            + "\n\n"
        )

    def add_note(self, note: str, indent: int = 0) -> None:
        """添加注释到 Markdown 文件

        Args:
            note (str): 注释内容
            indent (int): 缩进级别, 默认0
        """
        # 处理注释内部的每一行，保持缩进一致
        indentation = "  " * indent

        # 按行分割注释内容
        note_lines = note.split("\n")

        # 对每行应用缩进和引用符号
        indented_note = "\n".join(f"{indentation}> {line}" for line in note_lines)

        # 添加到内容中，并确保段落后有两个换行
        self.content += f"{indented_note}\n\n"

    def add_table(
        self,
        headers: list,
        rows: list,
        alignments: Union[list, None] = None,
        indent: int = 0,
    ) -> None:
        """添加表格到 Markdown 文件

        Args:
            headers (list): 表头内容
            rows (list): 表格行内容
            alignments (Union[list, None]): 对齐方式, 默认居中对齐, 可选值为 'left', 'center', 'right'
        """
        # | 1      |    2     |      3 |
        # | :----- | :------: | -----: |
        # | 左对齐 | 居中对齐 | 右对齐 |
        # |        |          |        |
        # |        |          |        |

        # 检查headers是否为空
        if not headers:
            MARKDOWN_LOGGER.warning("表头为空，无法创建表格")
            return

        # 设置默认对齐方式
        if alignments is None:
            alignments = ["center"] * len(headers)
        elif len(alignments) < len(headers):
            # 如果对齐方式数量不够，补充为居中对齐
            MARKDOWN_LOGGER.warning(
                f"对齐方式数量不足，使用默认居中对齐补充 {len(headers) - len(alignments)} 个"
            )
            alignments = alignments + ["center"] * (len(headers) - len(alignments))

        # 构建表头行
        header_row = "  " * indent + "| " + " | ".join(str(h) for h in headers) + " |"

        # 构建对齐行
        alignment_markers = []
        for align in alignments:
            if align.lower() == "left":
                alignment_markers.append(":----- ")
            elif align.lower() == "right":
                alignment_markers.append(" -----:")
            else:  # 默认居中对齐
                alignment_markers.append(" :-----: ")

        alignment_row = "  " * indent + "|" + "|".join(alignment_markers) + "|"

        # 构建数据行
        data_rows = []
        for row in rows:
            # 确保行中的元素数量与表头一致
            row_data = row[: len(headers)]
            if len(row_data) < len(headers):
                MARKDOWN_LOGGER.warning(
                    f"行数据数量不足，使用空字符串补充 {len(headers) - len(row_data)} 个"
                )
                row_data = row_data + [""] * (len(headers) - len(row_data))

            data_row = (
                "  " * indent + "| " + " | ".join(str(cell) for cell in row_data) + " |"
            )
            data_rows.append(data_row)

        # 将表格写入文件
        MARKDOWN_LOGGER.info("添加表格到Markdown文件")

        MARKDOWN_LOGGER.info(f"表格已添加，包含 {len(headers)} 列和 {len(rows)} 行")
        self.content += (
            header_row + "\n" + alignment_row + "\n" + "\n".join(data_rows) + "\n\n"
        )
        MARKDOWN_LOGGER.info(
            f"表格已添加到内容，包含 {len(headers)} 列和 {len(rows)} 行"
        )

    def add_local_image(
        self, image_path: Union[Path, str], alt_text: str = "Image", indent: int = 0
    ) -> None:
        """添加本地图片到 Markdown 文件

        Args:
            image_path (Union[Path, str]): 图片路径
            alt_text (str): 图片替代文本
            indent (int): 缩进级别, 默认0
        """
        # 如果 image_path是Path对象, 则说明添加的是一个本地绝对路径的图片
        if isinstance(image_path, Path):
            MARKDOWN_LOGGER.info(
                f"添加本地绝对路径图片: {image_path}, 图片替代文本: {alt_text}"
            )
        # 如果 image_path是str对象, 则说明添加的是一个本地相对路径的图片
        elif isinstance(image_path, str):
            MARKDOWN_LOGGER.info(
                f"添加本地相对路径图片: {image_path}, 图片替代文本: {alt_text}"
            )
        else:
            MARKDOWN_LOGGER.error(
                f"图片路径类型错误: {image_path}, 图片替代文本: {alt_text}"
            )
            return
        self.content += f"{'  ' * indent}![{alt_text}]({image_path})\n\n"

    def add_external_image(
        self, image_url: str, alt_text: str = "Image", indent: int = 0
    ) -> None:
        """添加外部图片到 Markdown 文件

        Args:
            image_url (str): 图片URL
            alt_text (str): 图片替代文本
            indent (int): 缩进级别, 默认0
        """
        self.content += f"{'  ' * indent}![{alt_text}]({image_url})\n\n"
