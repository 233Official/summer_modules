from pathlib import Path
import json
import os
import traceback
from . import summer_modules_logger


def write_dict_to_json_file(data: dict, filepath: Path, one_line=True):
    """将 dict 写入到 json 文件
    Args:
        data (dict): 要写入的 dict
        filepath (Path): 文件路径
        one_line (bool): 是否写入为一行，默认为 True
    """
    if one_line:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    else:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


def read_json_file_to_dict(filepath: Path):
    """读取 json 文件到 dict
    Args:
        filepath (Path): 文件路径
    Returns:
        dict: 读取的 dict
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def write_list_to_txt_file(data: list, filepath: Path):
    """将 list 写入到 txt 文件
    Args:
        data (list): 要写入的 list
        filepath (Path): 文件路径
    """
    with open(filepath, "w", encoding="utf-8") as f:
        for item in data:
            f.write(f"{item}\n")


def read_txt_file_to_list(filepath: Path):
    """读取 txt 文件到 list
    Args:
        filepath (Path): 文件路径
    Returns:
        list: 读取的 list
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = f.readlines()
    return [line.strip() for line in data]


# 获取所有JSON文件列表
def get_all_json_files(directory: Path) -> list:
    """
    获取目录下所有JSON文件的路径
    :param directory: 目录路径
    :return: JSON文件路径列表
    """
    if not directory.exists():
        summer_modules_logger.error(f"目录不存在: {directory}")
        return []

    if not directory.is_dir():
        summer_modules_logger.error(f"路径不是目录: {directory}")
        return []

    try:
        # 使用list comprehension直接获取所有JSON文件
        json_files = list(directory.glob("**/*.json"))
        summer_modules_logger.info(
            f"在 {directory} 中找到 {len(json_files)} 个JSON文件"
        )
        return json_files
    except Exception as e:
        stre_trace = traceback.format_exc()
        summer_modules_logger.error(f"获取JSON文件时出错: {e}\n{stre_trace}")
        return []


def find_chinese_font():
    """
    查找系统中可用的中文字体

    返回:
        字体路径或None
    """
    # 常见中文字体路径列表
    font_paths = [
        # Windows字体
        "C:/Windows/Fonts/msyh.ttc",  # 微软雅黑
        "C:/Windows/Fonts/simsun.ttc",  # 宋体
        "C:/Windows/Fonts/simhei.ttf",  # 黑体
        # Linux字体
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/arphic/uming.ttc",
        # macOS字体
        "/System/Library/Fonts/PingFang.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
    ]

    # 尝试一个个加载字体，直到找到可用的
    for font_path in font_paths:
        if os.path.exists(font_path):
            return font_path

    # 如果找不到任何中文字体，返回None
    return None
