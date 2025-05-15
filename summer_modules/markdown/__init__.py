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
        self.logger = MARKDOWN_LOGGER
        self.logger.info(f"Markdown file path: {self.markdown_file_path}")
        self.logger.info("Markdown module initialized.")

    def add_header(self, header: str, level: int = 1) -> None:
        """添加标题到 Markdown 文件
        :param header: 标题内容
        :param level: 标题级别, 1-6, 默认1
        :return: None
        """
        self.logger.info(f"Adding header: {header}, Level: {level}")
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            f.write(f"{'#' * level} {header}\n\n")
            self.logger.info(f"Header added: {header}, Level: {level}")

    def add_paragraph(self, paragraph: str) -> None:
        """添加段落到 Markdown 文件
        :param paragraph: 段落内容
        :return: None
        """
        self.logger.info(f"Adding paragraph: {paragraph}")
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            f.write(f"{paragraph}\n\n")
            self.logger.info(f"Paragraph added: {paragraph}")

    def add_code_block(self, code: str, language: str = "python") -> None:
        """添加代码块到 Markdown 文件
        :param code: 代码内容
        :param language: 代码语言, 默认python
        :return: None
        """
        self.logger.info(f"Adding code block: {code}, Language: {language}")
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            f.write(f"```{language}\n{code}\n```\n\n")
            self.logger.info(f"Code block added: {code}, Language: {language}")

    def add_list(self, items: list, ordered: bool = False) -> None:
        """添加列表到 Markdown 文件
        :param items: 列表内容
        :param ordered: 是否为有序列表, 默认False
        :return: None
        """
        self.logger.info(f"Adding list: {items}, Ordered: {ordered}")
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            for item in items:
                f.write(f"{'1.' if ordered else '-'} {item}\n")
            f.write("\n")
            self.logger.info(f"List added: {items}, Ordered: {ordered}")

    def add_note(self, note: str) -> None:
        """添加注释到 Markdown 文件
        :param note: 注释内容
        :return: None
        """
        self.logger.info(f"Adding note: {note}")
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            f.write(f"> {note}\n\n")
            self.logger.info(f"Note added: {note}")

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
            self.logger.warning("表头为空，无法创建表格")
            return

        # 设置默认对齐方式
        if alignments is None:
            alignments = ["center"] * len(headers)
        elif len(alignments) < len(headers):
            # 如果对齐方式数量不够，补充为居中对齐
            self.logger.warning(
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
                self.logger.warning(
                    f"行数据数量不足，使用空字符串补充 {len(headers) - len(row_data)} 个"
                )
                row_data = row_data + [""] * (len(headers) - len(row_data))

            data_row = "| " + " | ".join(str(cell) for cell in row_data) + " |"
            data_rows.append(data_row)

        # 将表格写入文件
        self.logger.info("添加表格到Markdown文件")
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            f.write(header_row + "\n")
            f.write(alignment_row + "\n")
            f.write("\n".join(data_rows) + "\n\n")

        self.logger.info(f"表格已添加，包含 {len(headers)} 列和 {len(rows)} 行")

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
            self.logger.info(
                f"添加本地绝对路径图片: {image_path}, 图片替代文本: {alt_text}"
            )
        # 如果 image_path是str对象, 则说明添加的是一个本地相对路径的图片
        elif isinstance(image_path, str):
            self.logger.info(
                f"添加本地相对路径图片: {image_path}, 图片替代文本: {alt_text}"
            )
        else:
            self.logger.error(
                f"图片路径类型错误: {image_path}, 图片替代文本: {alt_text}"
            )
            return
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            f.write(f"![{alt_text}]({image_path})\n")
            f.write("\n")
            self.logger.info(f"本地图片已添加: {image_path}, 图片替代文本: {alt_text}")

    def add_external_image(self, image_url: str, alt_text: str = "Image") -> None:
        """添加外部图片到 Markdown 文件
        :param image_url: 图片URL
        :param alt_text: 图片替代文本
        :return: None
        """
        self.logger.info(f"添加外部图片: {image_url}, 图片替代文本: {alt_text}")
        with open(self.markdown_file_path, "a", encoding="utf-8") as f:
            f.write(f"![{alt_text}]({image_url})\n")
            f.write("\n")
            self.logger.info(f"外部图片已添加: {image_url}, 图片替代文本: {alt_text}")
