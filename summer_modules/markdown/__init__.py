# markdown 模块,用于基本的 markdown 元素添加
from pathlib import Path

from summer_modules.logger import init_and_get_logger
from typing import Union

CURRENT_DIR = Path(__file__).parent.resolve()
MARKDOWN_LOGGER = init_and_get_logger(
    current_dir=CURRENT_DIR, logger_name="markdown_logger"
)


class Markdown:
    def __init__(self, markdown_file_path: Path):
        self.markdown_file_path = markdown_file_path
        """markdown 文件路径"""
        MARKDOWN_LOGGER.info(f"Markdown file path: {self.markdown_file_path}")
        self.content = ""
        """markdown 文件内容"""
        self.load()
        MARKDOWN_LOGGER.info("Markdown module initialized.")

    def load(self) -> None:
        """加载 Markdown 文件内容
        :return: None
        """
        MARKDOWN_LOGGER.info(f"加载 markdown 文件内容: {self.markdown_file_path}")
        if not self.markdown_file_path.exists():
            MARKDOWN_LOGGER.warning(f"Markdown 文件不存在: {self.markdown_file_path}")
            return

        with open(self.markdown_file_path, "r", encoding="utf-8") as f:
            self.content = f.read()
        MARKDOWN_LOGGER.info("已加载 Markdown 文件内容。")

    def save(self) -> None:
        """保存 Markdown 文件
        :return: None
        """
        MARKDOWN_LOGGER.info(f"保存 markdown 内容到 {self.markdown_file_path}")
        with open(self.markdown_file_path, "w", encoding="utf-8") as f:
            f.write(self.content)
        MARKDOWN_LOGGER.info("已保存 Markdown 文件。")
        self.content = ""  # 清空内容以便下次使用

    def clear_all(self) -> None:
        """清空 Markdown 文件内容以及本地 markdown 文件(如果存在)内容
        :return: None
        """
        MARKDOWN_LOGGER.info("清空 Markdown 文件内容和本地文件内容")
        self.content = ""
        if self.markdown_file_path.exists():
            self.markdown_file_path.unlink()
            MARKDOWN_LOGGER.info(f"已删除本地 Markdown 文件: {self.markdown_file_path}")
        else:
            MARKDOWN_LOGGER.warning(
                f"本地 Markdown 文件不存在: {self.markdown_file_path}"
            )

    def add_header(self, header: str, level: int = 1) -> None:
        """添加标题到 Markdown 文件
        :param header: 标题内容
        :param level: 标题级别, 1-6, 默认1
        :return: None
        """
        self.content += f"{'#' * level} {header}\n\n"

    def add_paragraph(self, paragraph: str) -> None:
        """添加段落到 Markdown 文件
        :param paragraph: 段落内容
        :return: None
        """
        self.content += f"{paragraph}\n\n"

    # 添加分隔符
    def add_horizontal_rule(self) -> None:
        """添加水平分隔线到 Markdown 文件
        :return: None
        """
        self.content += "\n\n---\n\n"

    def add_code_block(self, code: str, language: str = "python") -> None:
        """添加代码块到 Markdown 文件
        :param code: 代码内容
        :param language: 代码语言, 默认python
        :return: None
        """
        self.content += f"```{language}\n{code}\n```\n\n"

    def add_list(self, items: list, ordered: bool = False) -> None:
        """添加列表到 Markdown 文件
        :param items: 列表内容
        :param ordered: 是否为有序列表, 默认False
        :return: None
        """
        self.content += (
            "\n".join(f"{'1.' if ordered else '-'} {item}" for item in items) + "\n\n"
        )

    def add_note(self, note: str) -> None:
        """添加注释到 Markdown 文件
        :param note: 注释内容
        :return: None
        """
        self.content += f"> {note}\n\n"

    def add_table(
        self, headers: list, rows: list, alignments: Union[list, None] = None
    ) -> None:
        """添加表格到 Markdown 文件
        :param headers: 表头内容
        :param rows: 表格行内容
        :param alignments: 对齐方式, 默认居中对齐, 可选值为 'left', 'center', 'right'
        :return: None
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
        header_row = "| " + " | ".join(str(h) for h in headers) + " |"

        # 构建对齐行
        alignment_markers = []
        for align in alignments:
            if align.lower() == "left":
                alignment_markers.append(":----- ")
            elif align.lower() == "right":
                alignment_markers.append(" -----:")
            else:  # 默认居中对齐
                alignment_markers.append(" :-----: ")

        alignment_row = "|" + "|".join(alignment_markers) + "|"

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

            data_row = "| " + " | ".join(str(cell) for cell in row_data) + " |"
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
        self, image_path: Union[Path, str], alt_text: str = "Image"
    ) -> None:
        """添加本地图片到 Markdown 文件
        :param image_path: 图片路径
        :param alt_text: 图片替代文本
        :return: None
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
        self.content += f"![{alt_text}]({image_path})\n\n"

    def add_external_image(self, image_url: str, alt_text: str = "Image") -> None:
        """添加外部图片到 Markdown 文件
        :param image_url: 图片URL
        :param alt_text: 图片替代文本
        :return: None
        """
        self.content += f"![{alt_text}]({image_url})\n\n"
