from pathlib import Path
from typing import Union
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties as FP

from summer_modules.logger import init_and_get_logger
from summer_modules.utils import (
    find_chinese_font,
)

CURRENT_DIR = Path(__file__).parent.resolve()
CHARTS_LOGGER = init_and_get_logger(
    current_dir=CURRENT_DIR, logger_name="charts_logger"
)

chinese_font = find_chinese_font()
if chinese_font:
    font = FP(fname=chinese_font, size=12)
    plt.rcParams["font.family"] = font.get_name()
else:
    CHARTS_LOGGER.warning("没有找到中文字体,可能会导致中文显示不正常")


def plot_bar_chart(
    data: Union[list, dict], title: str, xlabel: str, ylabel: str, save_path: Path
) -> None:
    """
    绘制柱形图
    :param data: 数据，可以是列表或字典
    :param title: 图表标题
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :param save_path: 保存路径
    :return: None
    """
    if isinstance(data, dict):
        labels = list(data.keys())
        values = list(data.values())
    elif isinstance(data, list):
        labels = [f"Item {i}" for i in range(len(data))]
        values = data
    else:
        raise ValueError("数据格式不支持")

    plt.figure(figsize=(10, 6))
    plt.bar(labels, values, color="skyblue")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    # 标注柱形图上的数值
    for i, value in enumerate(values):
        plt.text(i, value + 0.1, str(value), ha="center", va="bottom")

    plt.tight_layout()

    # plt.show()
    plt.savefig(save_path)
    CHARTS_LOGGER.info(f"柱形图已保存到: {save_path}")


def plot_pie_chart(data: Union[list, dict], title: str, save_path: Path) -> None:
    """
    绘制饼图
    :param data: 数据，可以是列表或字典
    :param title: 图表标题
    :param save_path: 保存路径
    :return: None
    """
    if isinstance(data, dict):
        labels = list(data.keys())
        values = list(data.values())
    elif isinstance(data, list):
        labels = [f"Item {i}" for i in range(len(data))]
        values = data
    else:
        raise ValueError("数据格式不支持")

    plt.figure(figsize=(8, 8))
    plt.pie(values, labels=labels, autopct="%1.1f%%", startangle=140)
    plt.title(title)
    plt.axis("equal")  # 使饼图为圆形

    # plt.show()
    plt.savefig(save_path)
    CHARTS_LOGGER.info(f"饼图已保存到: {save_path}")
