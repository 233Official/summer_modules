from pathlib import Path
import json
import os
import traceback
import time
import random
from functools import wraps
import asyncio
import inspect
from typing import Callable, Tuple, Union, Optional, Any, Type, Dict

from . import summer_modules_logger


def write_dict_to_json_file(data: dict, filepath: Path, one_line: bool = True):
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


def handle_final_failure(exception, context):
    # 记录最终失败
    print(f"所有重试都失败了,报错: {exception}")
    print(f"最后一次尝试是第 {context['attempt']} 次")
    # 返回一个默认值或者处理结果
    return None


def retry(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff_strategy: str = "fixed",  # 'fixed', 'exponential', 'random'
    backoff_factor: float = 2.0,
    jitter: float = 0.1,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    should_retry: Optional[Callable[[Exception], bool]] = None,
    before_retry: Optional[Callable[..., None]] = None,
    after_retry: Optional[Callable[..., None]] = None,
    on_permanent_failure: Optional[Callable[..., Any]] = handle_final_failure,
):
    """
    通用的重试装饰器，支持多种重试策略和回调函数，同时支持同步和异步函数

    参数:
        max_retries: 最大重试次数
        delay: 初始延迟时间(秒)
        backoff_strategy: 退避策略 ('fixed', 'exponential', 'random')
        backoff_factor: 退避因子，用于计算指数退避
        jitter: 随机抖动因子，增加随机性避免重试风暴
        exceptions: 需要捕获并重试的异常类型
        should_retry: 函数，接收异常作为参数，返回是否应该重试
        before_retry: 重试前执行的回调函数，可用于资源清理、重置连接等
        after_retry: 重试后执行的回调函数
        on_permanent_failure: 永久失败后的回调函数，可用于执行替代逻辑(默认处理最终失败,输出相关信息), 可以通过将此参数设置为 None 来禁用永久失败处理

    返回:
        装饰器函数
    """

    def decorator(func):
        # 判断是否为异步函数
        is_async = inspect.iscoroutinefunction(func)

        # 异步函数处理逻辑
        if is_async:

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                retries = 0
                context = {"attempt": 0, "args": args, "kwargs": kwargs.copy()}

                while True:
                    context["attempt"] = retries + 1
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as e:
                        retries += 1
                        # 判断是否应该继续重试
                        if retries >= max_retries or (
                            should_retry and not should_retry(e)
                        ):
                            summer_modules_logger.error(
                                f"异步函数 {func.__name__} 执行失败，已达到最大重试次数或不满足重试条件"
                            )
                            if on_permanent_failure:
                                # 处理异步回调
                                if inspect.iscoroutinefunction(on_permanent_failure):
                                    return await on_permanent_failure(e, context)
                                else:
                                    return on_permanent_failure(e, context)
                            raise e

                        # 计算下次重试的等待时间
                        wait_time = calculate_wait_time(
                            delay, retries, backoff_strategy, backoff_factor, jitter
                        )

                        summer_modules_logger.warning(
                            f"异步函数 {func.__name__} 执行失败: {str(e)}"
                        )
                        summer_modules_logger.info(
                            f"{wait_time:.2f}秒后进行第{retries}/{max_retries}次重试"
                        )

                        # 执行重试前回调
                        if before_retry:
                            if inspect.iscoroutinefunction(before_retry):
                                await before_retry(context, e)
                            else:
                                before_retry(context, e)

                        # 使用异步睡眠
                        await asyncio.sleep(wait_time)

                        # 执行重试后回调
                        if after_retry:
                            if inspect.iscoroutinefunction(after_retry):
                                await after_retry(context)
                            else:
                                after_retry(context)

            return async_wrapper

        # 同步函数处理逻辑 (原有代码)
        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                retries = 0
                context = {"attempt": 0, "args": args, "kwargs": kwargs.copy()}

                while True:
                    context["attempt"] = retries + 1
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        retries += 1
                        # 判断是否应该继续重试
                        if retries >= max_retries or (
                            should_retry and not should_retry(e)
                        ):
                            summer_modules_logger.error(
                                f"函数 {func.__name__} 执行失败，已达到最大重试次数或不满足重试条件"
                            )
                            if on_permanent_failure:
                                return on_permanent_failure(e, context)
                            raise e

                        # 计算下次重试的等待时间
                        wait_time = calculate_wait_time(
                            delay, retries, backoff_strategy, backoff_factor, jitter
                        )

                        summer_modules_logger.warning(
                            f"函数 {func.__name__} 执行失败: {str(e)}"
                        )
                        summer_modules_logger.info(
                            f"{wait_time:.2f}秒后进行第{retries}/{max_retries}次重试"
                        )

                        # 执行重试前回调
                        if before_retry:
                            before_retry(context, e)

                        time.sleep(wait_time)

                        # 执行重试后回调
                        if after_retry:
                            after_retry(context)

            return sync_wrapper

    return decorator


def calculate_wait_time(delay, retry_count, strategy, factor, jitter):
    """计算下一次重试的等待时间"""
    if strategy == "fixed":
        wait = delay
    elif strategy == "exponential":
        wait = delay * (factor ** (retry_count - 1))
    elif strategy == "random":
        wait = random.uniform(delay, delay * factor)
    else:
        wait = delay

    # 添加抖动避免重试风暴
    if jitter > 0:
        wait = wait * (1 + random.uniform(-jitter, jitter))

    return max(0, wait)  # 确保等待时间不为负
