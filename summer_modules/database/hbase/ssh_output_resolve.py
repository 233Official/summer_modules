"""
解析 HBase Shell 扫描命令的输出
"""

import re
from pathlib import Path
import traceback
from typing import Any, Optional, Union

# 内存监控
import json
import os
from pympler import asizeof

from summer_modules.database.hbase import HBASE_LOGGER
from summer_modules.database.hbase.hbase_model import (
    HBaseColumn,
    HBaseRow,
    HBaseScanResult,
    ReconstructTruncatedLinesResult,
)
from summer_modules.utils import write_dict_to_json_file


# 解析 HBase Shell 扫描命令的输出
def parse_hbase_shell_scan_cmd_output(output: str) -> HBaseScanResult:
    """解析 HBase Shell 扫描命令的输出

    Args:
        table_name: 表名
        output: HBase Shell 扫描命令的输出字符串

    Returns:
        HBaseScanResult: 包含解析结果的对象
        - success: 是否成功解析
        - error_message: 错误信息（如果有）
        - table_name: 表名
        - command: 执行的命令
        - row_count: 扫描到的行数
        - execution_time: 执行时间（秒）
        - rows: list[HBaseRow]: 扫描到的行数据列表
    """
    result = HBaseScanResult(
        success=False,
        error_message="",
        table_name="",
        command="",
        row_count=0,
        execution_time=0.0,
        rows=[],
    )

    if not output or not output.strip():
        result.error_message = "HBase Shell 扫描命令没有输出任何信息, 这种情况理论上不应该发生, 请手动调试检查"
        HBASE_LOGGER.error(result.error_message)
        return result

    # 0.预处理: 重建被截断的行
    reconstructed_result = reconstruct_truncated_lines(output)

    # 1. 提取命令信息
    # 提取命令行, 从 scan 开始到类似 ROW                   COLUMN+CELL 模式之前的内容
    if not reconstructed_result.command_line:
        result.error_message = "无法从输出中提取命令行"
        HBASE_LOGGER.error(result.error_message)
        return result
    result.command = reconstructed_result.command_line.strip()
    ## 顺手提取一下 table_name scan 命令后的单引号内的部分即为表名
    table_name_match = re.search(r"scan\s+'([^']+)'", result.command)
    if table_name_match:
        result.table_name = table_name_match.group(1).strip()
    else:
        result.error_message = "无法从命令行中提取表名"
        HBASE_LOGGER.error(result.error_message)
        return result

    # 2. 检查是否有错误信息(等遇到再补充)
    # error_patterns = [
    #     r"ERROR",
    #     r"Exception",
    #     r"Table .* does not exist",
    #     r"Unknown table",
    # ]

    # for pattern in error_patterns:
    #     if re.search(pattern, output, re.IGNORECASE):
    #         result.error_message = f"HBase命令执行出错, 输出包含错误信息: {pattern}"
    #         return result

    # 3. 提取执行时间
    if not reconstructed_result.execution_time_line:
        result.error_message = "无法从输出中提取执行时间行"
        HBASE_LOGGER.error(result.error_message)
        return result
    time_pattern = r"Took\s+([\d.]+)\s+seconds"
    time_match = re.search(time_pattern, reconstructed_result.execution_time_line)
    if time_match:
        result.execution_time = float(time_match.group(1))
    else:
        result.error_message = "无法从输出中提取执行时间"
        HBASE_LOGGER.error(result.error_message)
        return result

    # 4. 提取行数统计
    if not reconstructed_result.row_count_line:
        result.error_message = "无法从输出中提取行数统计行"
        HBASE_LOGGER.error(result.error_message)
        return result
    row_count_pattern = r"(\d+)\s+row\(s\)"
    row_count_match = re.search(row_count_pattern, reconstructed_result.row_count_line)
    if not row_count_match:
        result.error_message = "无法从输出中提取行数统计"
        HBASE_LOGGER.error(result.error_message)
        return result
    if row_count_match:
        result.row_count = int(row_count_match.group(1))

    # 5. 解析数据行
    if not reconstructed_result.data_lines:
        result.error_message = "无法从输出中提取数据行"
        HBASE_LOGGER.error(result.error_message)
        return result

    extract_rows = extract_rows_from_reconstructed_data_lines(
        reconstructed_result.data_lines
    )
    if not extract_rows:
        result.error_message = "没有提取到任何有效的行数据"
        HBASE_LOGGER.error(result.error_message)
        return result
    result.rows = extract_rows

    result.success = True
    return result


# 从重建后的数据行列表中提取 list[HbaseRow]
def extract_rows_from_reconstructed_data_lines(data_lines: list[str]) -> list[HBaseRow]:
    """从重建后的数据行列表中提取 list[HBaseRow]

    Args:
        data_lines: 重建后的数据行列表
    Returns:
        list[HBaseRow]: 提取后的 HBaseRow 列表
    """
    # 根据相同的行键将 column 组织起来形成 row
    rows = []
    row_map = {}  # 用于存储行键和对应的列信息
    for data_line in data_lines:
        row_key, column = extract_row_key_and_column_from_reconstructed_data_line(
            data_line
        )
        if not row_key or not column:
            HBASE_LOGGER.warning(f"跳过无效的数据行: {data_line}")
            continue

        # 如果行键已经存在，则添加列信息
        if row_key in row_map:
            row_map[row_key].columns.append(column)
        else:
            # 如果行键不存在，则创建新的行并添加列信息
            new_row = HBaseRow(row_key=row_key, columns=[column])
            row_map[row_key] = new_row

    # 将 row_map 转换为 list[HBaseRow]
    for row_key, row in row_map.items():
        rows.append(row)

    if not rows:
        HBASE_LOGGER.warning("没有提取到任何有效的行数据")
    else:
        HBASE_LOGGER.debug(f"提取到 {len(rows)} 行数据")
        # HBASE_LOGGER.debug(f"提取的行数据: {[row.row_key for row in rows[:3]]}...")
    return rows


# 从重建后的数据行中提取行键与列信息
def extract_row_key_and_column_from_reconstructed_data_line(
    data_line: str,
) -> tuple[str, Optional[HBaseColumn]]:
    """从重建后的数据行中提取行键与列信息

    Args:
        data_line: 重建后的数据行字符串

    Returns:
        tuple: (行键, HBaseColumn)
    """
    # data_line 格式为 " row_key column=column_family:column_qualifier, timestamp=timestamp, value=value"
    parts = data_line.lstrip().split(" ", 1)
    if len(parts) < 2:
        HBASE_LOGGER.error(f"数据行格式不正确: {data_line}")
        return "", None
    row_key = parts[0].strip()  # 行键部分
    column_part = parts[1].strip()  # 列部分
    # 列部分格式为 "column=column_family:column_qualifier, timestamp=timestamp, value=value"
    column_parts = column_part.split(", ")
    if len(column_parts) < 3:
        HBASE_LOGGER.error(f"列部分格式不正确: {column_part}")
        return (row_key, None)
    column_info = column_parts[0].strip()  # 列信息部分
    # 提取列族和列限定符
    column_match = re.match(r"column=([^\s:]+):([^\s]+)", column_info)
    if not column_match:
        HBASE_LOGGER.error(f"列信息格式不正确: {column_info}")
        return (row_key, None)
    column_family = column_match.group(1).strip()  # 列族
    column_qualifier = column_match.group(2).strip()  # 列限定符
    # 提取时间戳和数值
    timestamp_part = column_parts[1].strip()  # 时间戳部分
    value_part = column_parts[2].strip()  # 数值部分
    timestamp_match = re.match(r"timestamp=(\d+)", timestamp_part)
    if not timestamp_match:
        HBASE_LOGGER.error(f"时间戳格式不正确: {timestamp_part}")
        return (row_key, None)
    timestamp = int(timestamp_match.group(1))  # 时间戳
    value_match = re.match(r"value=(.*)", value_part)
    if not value_match:
        HBASE_LOGGER.error(f"数值格式不正确: {value_part}")
        return (row_key, None)
    value = value_match.group(1)  # 数值

    # 创建 HBaseColumn 对象
    column = HBaseColumn(
        column_family=column_family,
        column_qualifier=column_qualifier,
        timestamp=timestamp,
        value=value,
    )
    return (row_key, column)


# 重建被截断的行
def reconstruct_truncated_lines(output: str) -> ReconstructTruncatedLinesResult:
    """重建被截断的行 - 基于精确的HBase格式分析
    分如下部分重建
    1.命令行 - scan 起始的部分
    2.ROW标题行 - 类似 ROW                   COLUMN+CELL 的格式
    3.数据行(需要精细处理)
    4.行数统计行 - x row(s)
    5.时间统计行 - Took x seconds
    6.后续命令行 - 类似 hbase(main):002:0> 的格式


    Args:
        output: 原始输出

    Returns:
        ReconstructTruncatedLinesResult: 包含重建结果的对象
    """
    # 标准化换行符
    normalized_output = output.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized_output.split("\n")

    reconstructed_lines = []

    command_line = ""
    row_title_line = ""
    data_lines = []
    row_count_line = ""
    execution_time_line = ""
    subsequent_command_line = ""

    i = 0

    while i < len(lines):
        current_line = lines[i].rstrip()

        # 处理命令行
        if re.match(r"scan\s+", current_line):
            command_parts = [current_line]
            i += 1
            while i < len(lines):
                next_line = lines[i].strip()
                if next_line.startswith("ROW") and "COLUMN+CELL" in next_line:
                    break
                if re.search(r"\d+\s+row\(s\)|Took\s+[\d.]+\s+seconds", next_line):
                    break
                if next_line:
                    command_parts.append(next_line)
                i += 1
            command_line = "".join(command_parts).strip()
            reconstructed_lines.append(command_line)
            continue

        # 处理ROW标题行
        elif current_line.startswith("ROW") and "COLUMN+CELL" in current_line:
            row_title_line = current_line.strip()
            reconstructed_lines.append(row_title_line)
            i += 1
            continue

        # 处理数据行(单个空格开头的行都是数据行)
        elif current_line.startswith(" "):
            data_lines.append(current_line)
            i += 1

        # 处理行数统计行 - 类似 "x row(s)"
        elif re.search(r"\d+\s+row\(s\)", current_line):
            row_count_line = current_line.strip()
            i += 1

        # 处理执行时间行 - 类似 "Took x seconds"
        elif re.search(r"Took\s+[\d.]+\s+seconds", current_line):
            execution_time_line = current_line.strip()
            i += 1

        # 处理后续命令行 - 类似 "hbase(main):002:0>"
        elif re.match(r"hbase\(main\):\d+:\d+>", current_line):
            subsequent_command_line = current_line.strip()
            i += 1

    # 集中处理数据行
    reconstructed_data_lines = reconstruct_complete_data_row(lines=data_lines)
    if reconstructed_data_lines:
        reconstructed_lines.extend(reconstructed_data_lines)
    else:
        HBASE_LOGGER.warning("没有找到任何有效的数据行")

    # 添加行数统计行
    if row_count_line:
        reconstructed_lines.append(row_count_line)
    # 添加执行时间行
    if execution_time_line:
        reconstructed_lines.append(execution_time_line)
    # 添加后续命令行
    if subsequent_command_line:
        reconstructed_lines.append(subsequent_command_line)

    reconstructed_output = "\n".join(reconstructed_lines)

    result = ReconstructTruncatedLinesResult(
        original=output,
        success=True,
        command_line=command_line,
        row_title_line=row_title_line,
        data_lines=reconstructed_data_lines,
        row_count_line=row_count_line,
        execution_time_line=execution_time_line,
        subsequent_command_line=subsequent_command_line,
        reconstructed=reconstructed_output,
    )

    return result


# 判断一个被截断的行内容是否为数据行的开始
def is_data_row_start(line: str) -> bool:
    """判断是否是数据行的开始

    数据行的开始特征：(一个空格开头) + (行键开始部分) + (一个空格分隔) + (列起始部分column=xxx)
    """
    re_match_result = re.match(r"^\s*([^\s]+(?:\s+[^\s]+)*)\s+column=", line)
    if re_match_result:
        return True
    return False


# 重建完整的数据行
def reconstruct_complete_data_row(lines: list) -> Optional[list]:
    """重建完整的数据行

    策略：
    1. 识别起始行 - (一个空格开头) + (行键开始部分) + (一个空格分隔) + (列起始部分column=xxx) - 根据起始行将 lines 区分为若干个确定行列表
    2. 处理每个行列表拼装成行
    3. 重整完整的数据行

    Args:
        lines: 原始行列表(ssh 接口获取到的命令执行输出处理后的数据行)
    Returns:
        Optional[list]: 重建后的完整数据行列表
        如果没有找到任何数据行，则返回 None
    """

    # 确定的行嵌套列表, 每个元素都代表一个行的所有行列表
    stand_lines = []

    lines_num = len(lines)
    lines_index = 0

    while lines_index < lines_num:
        line = lines[lines_index].rstrip()
        start_line_flag = False  # 已找到起始行的标记
        stand_line = []  # 标准行的所有相关行
        if is_data_row_start(line):
            start_line_flag = True
            # 如果是数据行的开始，收集这个行的所有相关行
            stand_line.append(line)
            lines_index += 1
        # 如果是数据行的开始，收集这个行的所有相关行
        if start_line_flag:
            # 收集后续行，直到遇到下一个数据行开始
            while lines_index < lines_num:
                next_line = lines[lines_index].rstrip()

                # 空行跳过
                if not next_line.strip():
                    lines_index += 1
                    continue

                # 遇到下一个数据行开始，停止并将当前行列表添加到标准行列表
                if is_data_row_start(next_line):
                    break

                # 收集当前行
                stand_line.append(next_line)
                lines_index += 1

            # HBASE_LOGGER.debug(f"已匹配标准数据行列表: {stand_line}")
            stand_lines.append(stand_line)
            stand_line = []  # 重置当前行列表
            start_line_flag = False

        else:
            HBASE_LOGGER.error(
                "当前行并非数据行的开始, 理论上不会出现这种情况, 如果程序运行到这里, 说明代码逻辑有问题, 请检查"
            )
            return None

    # HBASE_LOGGER.debug(f"所有标准行列表: {stand_lines}")

    # 重建完整的数据行
    reconstructed_lines = []
    for stand_line in stand_lines:
        reconstructed_line = reconstruct_single_data_row(stand_line)
        if reconstructed_line:
            reconstructed_lines.append(reconstructed_line)

    # HBASE_LOGGER.debug(f"重建后的完整数据行: {reconstructed_lines}")

    return reconstructed_lines if reconstructed_lines else None


# 根据单个数据行列表重建单个数据行
def reconstruct_single_data_row(stand_line: list[str]) -> str:
    """根据单个数据行列表重建单个数据行
    处理逻辑:
    1. 第一个必定是首行, 模式必定是 (一个空格开头) + (行键开始部分) + (一个空格分隔) + (列起始部分column=xxx) 这样可以抽离出 row_key 与 column 信息
    2. 处理后续行, 若模式为 (一个空格开头) + (行键后续部分) + (若干空格结尾) 则说明这一行只有行键后续部分, 需要将其拼接到首行的 row_key 部分
    3. 若模式为 (一个空格开头) + (行键后续部分) + (一个空格分隔) + (列后续部分) 则根据第二个空格将其分为 row_key 后续部分与列后续部分, 列后续部分需要拼接到 column 信息中
    4. 若模式为 (一个空格开头) + (行键后续部分) + (若干空格分隔) + (列后续部分) 则根据第二次出现的空格将其分为 row_key 后续部分与列后续部分, 列后续部分需要拼接到 column 信息中
    5. 若模式为 (若干空格开头) + (列后续部分) 则说明这一行是列后续部分, 需要将其拼接到 column 信息中


    Args:
        stand_line: 单个数据行的所有相关行列表
    Returns:
        str: 重建后的单个数据行字符串
    """
    if not stand_line or len(stand_line) == 0:
        HBASE_LOGGER.error("传入的行列表为空或无效, 无法重建数据行")
        return ""

    # 初始化行键和列信息
    row_key = ""
    column = ""
    first_line = stand_line[0].rstrip()
    # 首行必定是 (一个空格开头) + (行键开始部分) + (一个空格分隔) + (列起始部分column=xxx) 根据第二个空格抽离出 row_key 与 column 信息
    parts = first_line.lstrip().split(" ", 1)
    if len(parts) < 2:
        HBASE_LOGGER.error(
            "首行格式不正确, 无法从首行中提取行键和列信息: {}".format(first_line)
        )
        return ""
    row_key = parts[0].strip()  # 行键部分
    column = parts[1].strip()  # 列部分

    line_index = 1
    while line_index < len(stand_line):
        line = stand_line[line_index].rstrip()

        # 如果行是空的，跳过
        if not line.strip():
            line_index += 1
            continue

        # 检查行的模式
        # 若以多个空格开头, 则此行必定只有列后续部分, 清除前缀的所有空格然后将后续部分拼接到 column 信息中
        if line.startswith(" " * 2):
            column += line.strip()  # 清除前缀空格并拼接到列信息中
            # HBASE_LOGGER.debug(
            #     f"匹配到(多个空格) + (列后续部分), 列后续部分: {line.strip()}"
            #     f"列后续部分为: {line.strip()}"
            # )
            line_index += 1
        # 如果行以一个空格开头, 则需要区分更复杂的情况
        elif line.startswith(" "):
            # 若模式为 (一个空格开头) + (行键后续部分) + (若干空格结尾) 则说明这一行只有行键后续部分, 需要将其拼接到首行的 row_key 部分
            if re.match(r"\s[^\s]+\s*$", line):
                row_key += line.strip()
                # HBASE_LOGGER.debug(
                #     f"匹配到(一个空格) + (行键后续部分) + (若干空格结尾), 行键后续部分: {line.strip()}"
                # )
                line_index += 1
            # 若模式为 (一个空格开头) + (行键后续部分) + (一个空格分隔) + (列后续部分[若干字符串(并不一定以 column 开头)])
            # 正则为: (一个空格) + (若干字符串) + (一个空格) + (若干字符串)
            elif re.match(r"\s[^\s]+\s[^\s]+", line):
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    HBASE_LOGGER.error(
                        "行格式不正确, 无法从行中提取行键后续部分和列后续部分: {}".format(
                            line
                        )
                    )
                    line_index += 1
                    continue
                row_key += parts[1].strip()  # 拼接行键
                column += parts[2].strip()  # 拼接列信息
                # HBASE_LOGGER.debug(
                #     f"匹配到(一个空格) + (若干字符串) + (一个空格) + (若干字符串)"
                #     f"行键后续部分: {parts[1].strip()}, 列后续部分: {parts[2].strip()}"
                # )
                line_index += 1
            # 若模式为 (一个空格开头) + (行键后续部分) + (若干空格分隔) + (列后续部分)
            # 正则为: (一个空格) + (若干字符串) + (若干空格) + (若干字符串)
            # 则将第一个空格后续的字符串直到第二个空格为止作为行键后续部分, 第二个空格后面的字符串作为列后续部分
            elif re.match(r"\s[^\s]+[ \t\r\f\v]*[^\s]+", line):
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    HBASE_LOGGER.error(
                        "行格式不正确, 无法从行中提取行键后续部分和列后续部分: {}".format(
                            line
                        )
                    )
                    line_index += 1
                    continue
                row_key += parts[1].strip()  # 拼接行键
                column += parts[2].strip()  # 拼接列信息
                # HBASE_LOGGER.debug(
                #     f"匹配到(一个空格) + (若干字符串) + (若干空格) + (若干字符串)"
                #     f"行键后续部分: {parts[1].strip()}, 列后续部分: {parts[2].strip()}"
                # )
                line_index += 1
            else:
                HBASE_LOGGER.error("行格式不正确, 无法处理行: {}".format(line))
                line_index += 1
                continue
        else:
            HBASE_LOGGER.error("行格式不正确, 无法处理行: {}".format(line))
            line_index += 1
            continue

    # 拼接最终的行字符串
    reconstructed_line = f" {row_key} {column}"

    # HBASE_LOGGER.debug(f"重建的单个数据行: {reconstructed_line}")
    return reconstructed_line


# 解析手动全量数据导出文件并转为 json
def parse_manual_full_export_file_to_json(
    src_filepath: Path,
    out_json_filepath: Optional[Path] = None,
    data_size_limit: Optional[int] = None,
) -> Optional[Path]:
    """解析手动全量数据导出文件并转为 JSON

    Args:
        src_filepath: 手动全量数据导出文件路径
        out_json_filepath: 输出的 JSON 文件路径, 如果为 None 则默认与输入文件同名但扩展名为 .json
        data_size_limit(developing......): (还有 bug 没有解完, 待后续优化, 暂时不要用这个参数)数据大小限制, 每当数据超过此限制则写入 JSON 文件并清空内存, 默认 None 不限制大小
    Returns:
        Optional[Path]: 输出的 JSON 文件路径, 如果转换失败则返回 None
    """
    if not src_filepath.exists() or not src_filepath.is_file():
        HBASE_LOGGER.error(f"输入文件不存在或不是文件: {src_filepath}")
        return None

    if out_json_filepath is None:
        out_json_filepath = src_filepath.with_suffix(".json")

    # 内存监控

    rows = {}

    try:
        # 一般全量导出文件都比较大, 因此这里边读边写, 不做一次性读取
        with open(src_filepath, "r", encoding="utf-8") as infile, open(
            out_json_filepath, "w", encoding="utf-8"
        ) as outfile:
            line_num = 1  # 行号, 从1开始
            find_start_flag = False  # 是否找到 ROW  COLUMN+CELL 的格式
            start_line_num = 0  # 起始行号, 用于记录找到起始行的行号
            find_end_flag = False  # 是否找到行数统计行 xxx row(s)
            end_line_num = 0  # 结束行号, 用于记录找到结束行的行号
            part_index = (
                0  # 如果数据量过大, 则分多次写入, 这个变量用于记录当前是第几次写入
            )
            part_manifest = {
                "total_parts": 0,  # 总共的分块数
                "row_num": 0,  # 当前分块的行数
                "part_filepaths": [],  # 当前分块的文件路径列表
            }  # 用于记录每次写入的文件名和行数和块数
            every_row_column_num = 0  # 每行数据的列数
            for line in infile:
                if line_num % 1000 == 0:
                    # 每1000行输出一次日志
                    HBASE_LOGGER.debug(f"正在处理第 {line_num} 行....")
                if not find_start_flag:
                    # 如果当前行匹配到 ROW  COLUMN+CELL 的格式, 则标记找到起始行
                    if re.match(r"^\s*ROW\s+COLUMN\+CELL", line):
                        find_start_flag = True
                        HBASE_LOGGER.info(
                            f"找到起始行: {line.strip()} (行号: {line_num})"
                        )
                        start_line_num = line_num
                        line_num += 1
                        continue
                    else:
                        # 如果当前行不是起始行, 则跳过
                        line_num += 1
                        HBASE_LOGGER.debug(
                            f"跳过非起始行: {line.strip()} (行号: {line_num})"
                        )
                        continue
                # 如果当前已找到起始行但未找到结束行
                elif find_start_flag and not find_end_flag:
                    # 如果当前行匹配到行数统计行 xxx row(s), 则标记找到结束行
                    if re.match(r"^\s*\d+\s+row\(s\)", line):
                        find_end_flag = True
                        HBASE_LOGGER.info(
                            f"找到结束行: {line.strip()} (行号: {line_num})"
                        )
                        end_line_num = line_num
                        line_num += 1
                    # 如果当前行是数据行, 则解析并添加到 rows 中
                    elif re.match(r"^\s*[^\s]+", line):
                        # 提取行键和列信息
                        parts = line.strip().split(" ", 1)
                        if len(parts) < 2:
                            HBASE_LOGGER.warning(
                                f"跳过无效数据行: {line.strip()} (行号: {line_num})"
                            )
                            line_num += 1
                        row_key = parts[0].strip()
                        column_info = parts[1].strip()
                        # 进一步提取 column 信息 列族, 列限定符, 时间戳和数值
                        column_parts = column_info.split(", ", maxsplit=2)
                        if len(column_parts) < 3:
                            HBASE_LOGGER.warning(
                                f"跳过无效列信息: {column_info} (行号: {line_num})"
                            )
                            line_num += 1
                            continue
                        if len(column_parts) > 3:
                            HBASE_LOGGER.warning(
                                f"列信息格式异常, 可能包含多余的部分: {column_info} (行号: {line_num})"
                            )
                            line_num += 1
                            continue
                        column_info = column_parts[0].strip()
                        timestamp_info = column_parts[1].strip()
                        value_info = column_parts[2].strip()
                        # 提取列族和列限定符
                        column_match = re.match(
                            r"column=([^\s:]+):([^\s]+)", column_info
                        )
                        if not column_match:
                            HBASE_LOGGER.warning(
                                f"跳过无效列信息格式: {column_info} (行号: {line_num})"
                            )
                            line_num += 1
                            continue
                        column_family = column_match.group(1).strip()
                        column_qualifier = column_match.group(2).strip()
                        # 提取时间戳
                        timestamp_match = re.match(r"timestamp=(\d+)", timestamp_info)
                        if not timestamp_match:
                            HBASE_LOGGER.warning(
                                f"跳过无效时间戳格式: {timestamp_info} (行号: {line_num})"
                            )
                            line_num += 1
                            continue
                        timestamp = int(timestamp_match.group(1).strip())
                        # 提取数值
                        value_match = re.match(r"value=(.*)", value_info)
                        if not value_match:
                            HBASE_LOGGER.warning(
                                f"跳过无效数值格式: {value_info} (行号: {line_num})"
                            )
                            line_num += 1
                            continue
                        value = value_match.group(1).strip()

                        # 将行键和列信息添加到 rows 中
                        if row_key not in rows:
                            rows[row_key] = {}
                        if column_family not in rows[row_key]:
                            rows[row_key][column_family] = {}
                        rows[row_key][column_family][column_qualifier] = {
                            "timestamp": timestamp,
                            "value": value,
                        }
                        line_num += 1

                        # 判断当前 rows 的大小是否超过限制, 如果超过则写入 JSON 文件并清空内存, 每 10000行检查一次, 因为占用检查很耗时间
                        if data_size_limit is not None:
                            if line_num % 10000 != 0:
                                continue
                            if part_index == 0:
                                first_row_key = next(iter(rows))
                                every_row_column_num = sum(
                                    len(rows[first_row_key][column_family])
                                    for column_family in rows[first_row_key]
                                )

                            current_data_size = asizeof.asizeof(rows)
                            if current_data_size > data_size_limit:
                                HBASE_LOGGER.info(
                                    f"当前数据大小 {current_data_size} 字节, 超过限制 {data_size_limit} 字节, 写入 JSON 文件并清空内存"
                                )

                                # 如果 rows 的最后一条数据的列数并不完整则需要暂存
                                current_row_column_num = sum(
                                    len(rows[row_key][column_family])
                                    for column_family in rows[row_key]
                                )
                                stash_rows = {}
                                if current_row_column_num < every_row_column_num:
                                    stash_rows[row_key] = rows[row_key]
                                    # 从 rows 中删除当前行
                                    del rows[row_key]
                                elif current_row_column_num == every_row_column_num:
                                    pass
                                else:
                                    HBASE_LOGGER.error(
                                        f"当前行 {row_key} 的列数 {current_row_column_num} 大于 每行列数 {every_row_column_num}, 程序存在问题, 请手动调试检查"
                                    )

                                part_index_str = f"{part_index:03d}"  # 格式化为三位数
                                out_json_filepath_part = f"{out_json_filepath.stem}_part_{part_index_str}.json"
                                out_json_filepath_part = out_json_filepath.with_name(
                                    out_json_filepath_part
                                )
                                write_dict_to_json_file(
                                    data=rows,
                                    filepath=Path(out_json_filepath_part),
                                    one_line=True,
                                )
                                part_index += 1
                                part_manifest["part_filepaths"].append(
                                    out_json_filepath_part.__str__()
                                )
                                part_manifest["row_num"] += len(rows)
                                rows.clear()  # 清空 rows, 但保留 stash_rows
                                rows = stash_rows  # 清空 rows, 但保留 stash_rows
                    else:
                        # 如果当前行不是数据行, 则跳过
                        HBASE_LOGGER.warning(
                            f"跳过非数据行: {line.strip()} (行号: {line_num})"
                        )
                        line_num += 1
                # 如果当前已找到起始行和结束行则将 rows 写入 JSON 文件
                elif find_start_flag and find_end_flag:
                    if data_size_limit and part_index > 0:
                        # 如果数据量过大, 则写入 JSON 文件并清空内存
                        part_index_str = f"{part_index:03d}"  # 格式化为三位数
                        out_json_filepath_part = (
                            f"{out_json_filepath.stem}_part_{part_index_str}.json"
                        )
                        out_json_filepath_part = out_json_filepath.with_name(
                            out_json_filepath_part
                        )
                        write_dict_to_json_file(
                            data=rows,
                            filepath=Path(out_json_filepath_part),
                            one_line=True,
                        )
                        part_index += 1
                        part_manifest["part_filepaths"].append(
                            out_json_filepath_part.__str__()
                        )
                        part_manifest["row_num"] += len(rows)
                        # rows.clear() # 最后一次写入不清空内存, 因为最后一次写入可能是全部数据
                    else:
                        write_dict_to_json_file(
                            data=rows, filepath=out_json_filepath, one_line=False
                        )
                        one_line_out_json_filepath = out_json_filepath.with_suffix(
                            ".one_line.json"
                        )
                        write_dict_to_json_file(
                            data=rows,
                            filepath=one_line_out_json_filepath,
                            one_line=True,
                        )

        first_row_key = next(iter(rows))
        every_row_column_num = sum(
            len(rows[first_row_key][column_family])
            for column_family in rows[first_row_key]
        )

        if data_size_limit and part_index > 0:
            lines_from_json = part_manifest["row_num"]
            write_dict_to_json_file(
                data=part_manifest,
                filepath=out_json_filepath.with_suffix(".manifest.json"),
                one_line=False,
            )
        else:
            lines_from_json = len(rows)

        HBASE_LOGGER.info(
            f"解析完成, 共找到 {len(rows)} 行数据, 每行包含 {every_row_column_num} 列"
        )
        lines_from_line_num = (end_line_num - start_line_num - 1) / every_row_column_num
        if lines_from_line_num != lines_from_json:
            HBASE_LOGGER.warning(
                f"行数不匹配: 起始行到结束行的行数为 {lines_from_line_num}, 解析到的行数为 {lines_from_json}"
            )
        else:
            HBASE_LOGGER.info(
                f"行数匹配: 起始行号 {start_line_num}, 结束行号 {end_line_num}, "
                f"起始行到结束行之间的数据行数为 {lines_from_line_num}, 解析到的行数为 {lines_from_json}"
            )
        HBASE_LOGGER.info(f"转换成功，输出文件: {out_json_filepath}")
        return out_json_filepath
    except Exception as e:
        stack_trace = traceback.format_exc()
        HBASE_LOGGER.error(f"转换失败: {e}, 堆栈信息:\n{stack_trace}")
        return None


HEX_ESCAPE_PATTERN = re.compile(r"\\x([0-9A-Fa-f]{2})")


# 将 hbase shell 输出的 json 字符串格式化为json 字典
def format_hbase_shell_json_output(json_str: str) -> Optional[dict]:
    """将 hbase shell 输出的 json 字符串格式化为 json 字典

    Args:
        json_str: hbase shell 输出的 json 字符串

    Returns:
        Optional[dict]: 格式化后的 json 字典, 如果解析失败则返回 None
    """
    try:

        def replace_hex_escape(match):
            hex_code = match.group(1)
            return chr(int(hex_code, 16))

        # 匹配 \xXX 格式的转义字符
        processed_string = HEX_ESCAPE_PATTERN.sub(replace_hex_escape, json_str)

        # json.loads() 会自动处理标准转义字符：
        # \n, \t, \r, \\, \", \/, \b, \f
        # 以及 \uXXXX (Unicode) 和 \xXX (十六进制)
        json_data = json.loads(processed_string)  # 尝试将字符串解析为 JSON
        return json_data
    except json.JSONDecodeError as e:
        HBASE_LOGGER.error(f"JSON 解析失败:")
        HBASE_LOGGER.error(f"  错误消息: {e.msg}")
        HBASE_LOGGER.error(f"  错误位置: 第 {e.lineno} 行, 第 {e.colno} 列")
        HBASE_LOGGER.error(f"  错误字符位置: {e.pos}")

        # 显示出错位置附近的内容
        start = max(0, e.pos - 50)
        end = min(len(json_str), e.pos + 50)
        context = json_str[start:end]
        error_pos_in_context = e.pos - start
        pointer = " " * error_pos_in_context + "^"

        HBASE_LOGGER.error(f"  出错位置上下文:\n{context}\n{pointer}")
        return None
    except Exception as e:
        HBASE_LOGGER.error(f"解析 WHOIS 字符串失败: {e}")
        return None
