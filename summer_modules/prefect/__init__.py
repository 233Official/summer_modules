import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import traceback
import asyncio
import inspect


from prefect.schedules import Interval, Cron
from prefect.client import get_client
from prefect.docker.docker_image import DockerImage
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect.exceptions import ObjectNotFound

from summer_modules.logger.logger_prefect import init_and_get_logger

CURRENT_DIR = Path(__file__).parent.resolve()
SUMMER_PREFECT_LOGGER = init_and_get_logger(
    current_dir=CURRENT_DIR,
    logger_name="summer_prefect_logger",
)


def export_poetry_requirements_to_pips(project_base_dir: Path):
    """导出 Poetry 依赖到 requirements.txt"""
    requirements_txt_filepath = project_base_dir / "requirements.txt"
    # 使用 poetry export -f requirements.txt --output requirements.txt --without-hashes 导出 poetry 依赖
    subprocess.run(
        [
            "poetry",
            "export",
            "-f",
            "requirements.txt",
            "--output",
            str(requirements_txt_filepath),
            "--without-hashes",
        ],
        check=True,
    )
    SUMMER_PREFECT_LOGGER.info(f"已将 Poetry 依赖导出到 {requirements_txt_filepath}")


async def check_work_pool(work_pool_name: str, project_base_dir: Path):
    """检查工作池是否存在，如果不存在则创建"""
    async with get_client() as client:
        try:
            await client.read_work_pool(work_pool_name=work_pool_name)
            SUMMER_PREFECT_LOGGER.info(f"工作池 '{work_pool_name}' 已存在")
        except ObjectNotFound:
            SUMMER_PREFECT_LOGGER.warning(
                f"工作池 '{work_pool_name}' 不存在，正在创建..."
            )

            # # 打印方法签名
            # print(inspect.signature(client.create_work_pool))
            # # 或查看完整帮助信息
            # help(client.create_work_pool)

            # TODO: 有问题这种创建方案
            # work_pool_create = WorkPoolCreate(
            #     name=work_pool_name,
            #     type="docker",
            #     description=f"工作池 - {work_pool_name}",
            # )
            # await client.create_work_pool(work_pool=work_pool_create)

            # 首先需要激活项目根目录下的 source .venv/bin/activate 才能使用 prefect 命令, 然后直接使用 prefect work-pool create [pool_name] --type docker 创建 work_pool
            # 首先创建激活虚拟环境并运行 prefect 命令的完整命令
            cmd = f"cd {project_base_dir} && source .venv/bin/activate && prefect work-pool create {work_pool_name} --type docker"

            # 使用 shell=True 以支持 source 命令和环境变量
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            SUMMER_PREFECT_LOGGER.info(f"工作池 '{work_pool_name}' 创建完成")
        except Exception as e:
            SUMMER_PREFECT_LOGGER.info(f"type(e): {type(e)}, str(e): {str(e)}")
            SUMMER_PREFECT_LOGGER.error(
                f"检查工作池 '{work_pool_name}' 时出错: {e}\n报错堆栈:\n{traceback.format_exc()}"
            )
            raise e


# 同步包装函数，方便在非异步环境中调用
def check_work_pool_sync(work_pool_name: str, project_base_dir: Path):
    """检查工作池是否存在的同步版本，如果不存在则创建"""
    return asyncio.run(check_work_pool(work_pool_name, project_base_dir))


async def check_flow_deployment(deployment_name: str) -> bool:
    """检查流部署是否存在, 存在则返回 True, 不存在则返回 False"""
    async with get_client() as client:
        try:
            # 获取所有部署并过滤名称
            deployments = await client.read_deployments()
            matching_deployments = [d for d in deployments if d.name == deployment_name]

            if matching_deployments:
                SUMMER_PREFECT_LOGGER.info(f"流部署 '{deployment_name}' 已存在")
                return True
            else:
                SUMMER_PREFECT_LOGGER.warning(f"流部署 '{deployment_name}' 不存在")
                return False
        except Exception as e:
            SUMMER_PREFECT_LOGGER.error(
                f"检查流部署 '{deployment_name}' 时出错: {e}\n报错堆栈:\n{traceback.format_exc()}"
            )
            raise


def check_flow_deployment_sync(deployment_name: str) -> bool:
    """检查流部署是否存在的同步版本, 存在则返回 True, 不存在则返回 False"""
    return asyncio.run(check_flow_deployment(deployment_name))
