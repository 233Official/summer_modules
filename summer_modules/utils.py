from pathlib import Path
import json


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
