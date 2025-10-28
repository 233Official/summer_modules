from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Callable, Sequence

from prefect.client import get_client
from prefect.exceptions import ObjectNotFound

from .logger_prefect import init_and_get_logger

CURRENT_DIR = Path(__file__).resolve().parent
SUMMER_PREFECT_LOGGER = init_and_get_logger(
    current_dir=CURRENT_DIR,
    logger_name="summer_prefect_logger",
)

CommandRunner = Callable[
    [Sequence[str], Path | None], subprocess.CompletedProcess[str]
]


def _run_command(command: Sequence[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        check=True,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
    )


def export_poetry_requirements_to_pips(
    project_base_dir: Path,
    *,
    output_filename: str = "requirements.txt",
    command_runner: CommandRunner = _run_command,
) -> Path:
    """
    将 Poetry 项目依赖导出到 ``requirements.txt``。

    Args:
        project_base_dir: Poetry 项目的根目录。
        output_filename: 输出文件名称，默认为 ``requirements.txt``。
        command_runner: 可选的子进程执行函数，便于测试或自定义执行逻辑。

    Returns:
        生成的 requirements 文件路径。
    """
    output_path = project_base_dir / output_filename
    command = [
        "poetry",
        "export",
        "-f",
        "requirements.txt",
        "--output",
        str(output_path),
        "--without-hashes",
    ]
    result = command_runner(command, project_base_dir)
    SUMMER_PREFECT_LOGGER.info(
        "已将 Poetry 依赖导出到 %s", output_path, extra={"info_color": "green"}
    )
    if result.stdout:
        SUMMER_PREFECT_LOGGER.debug("poetry export 输出: %s", result.stdout.strip())
    if result.stderr:
        SUMMER_PREFECT_LOGGER.debug("poetry export 错误输出: %s", result.stderr.strip())
    return output_path


async def check_work_pool(
    work_pool_name: str,
    project_base_dir: Path,
    *,
    command_runner: CommandRunner = _run_command,
    client_factory: Callable[[], object] = get_client,
    work_pool_type: str = "docker",
) -> bool:
    """
    检查 Prefect 工作池是否存在，若不存在则尝试创建。

    Args:
        work_pool_name: 工作池名称。
        project_base_dir: 项目根目录，用于执行 CLI 命令。
        command_runner: 可选的子进程执行函数。
        client_factory: Prefect 客户端工厂，默认使用 ``prefect.client.get_client``。
        work_pool_type: 创建工作池时的类型，默认 ``docker``。

    Returns:
        bool: ``True`` 表示创建了新的工作池，``False`` 表示已存在。
    """

    async with client_factory() as client:  # type: ignore[func-returns-value]
        try:
            await client.read_work_pool(work_pool_name=work_pool_name)  # type: ignore[attr-defined]
            SUMMER_PREFECT_LOGGER.info(
                "工作池 '%s' 已存在", work_pool_name, extra={"info_color": "green"}
            )
            return False
        except ObjectNotFound:
            SUMMER_PREFECT_LOGGER.warning(
                "工作池 '%s' 不存在，正在创建...", work_pool_name
            )
            command = [
                "prefect",
                "work-pool",
                "create",
                work_pool_name,
                "--type",
                work_pool_type,
            ]
            result = command_runner(command, project_base_dir)
            if result.stdout:
                SUMMER_PREFECT_LOGGER.debug(
                    "prefect work-pool create 输出: %s", result.stdout.strip()
                )
            if result.stderr:
                SUMMER_PREFECT_LOGGER.debug(
                    "prefect work-pool create 错误输出: %s", result.stderr.strip()
                )
            SUMMER_PREFECT_LOGGER.info(
                "工作池 '%s' 创建完成", work_pool_name, extra={"info_color": "green"}
            )
            return True
        except Exception as exc:  # pragma: no cover - 运行时记录真实异常
            SUMMER_PREFECT_LOGGER.error(
                "检查/创建工作池 '%s' 时发生异常: %s", work_pool_name, exc
            )
            raise


def check_work_pool_sync(
    work_pool_name: str,
    project_base_dir: Path,
    *,
    command_runner: CommandRunner = _run_command,
    client_factory: Callable[[], object] = get_client,
    work_pool_type: str = "docker",
) -> bool:
    """
    ``check_work_pool`` 的同步包装。
    """
    return asyncio.run(
        check_work_pool(
            work_pool_name=work_pool_name,
            project_base_dir=project_base_dir,
            command_runner=command_runner,
            client_factory=client_factory,
            work_pool_type=work_pool_type,
        )
    )


async def check_flow_deployment(
    deployment_name: str,
    *,
    client_factory: Callable[[], object] = get_client,
) -> bool:
    """
    检查 Prefect 部署是否存在。

    Args:
        deployment_name: 部署名称。
        client_factory: Prefect 客户端工厂。

    Returns:
        bool: ``True`` 表示部署存在，``False`` 表示未找到。
    """
    async with client_factory() as client:  # type: ignore[func-returns-value]
        try:
            deployments = await client.read_deployments()  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover
            SUMMER_PREFECT_LOGGER.error(
                "读取部署列表时发生异常: %s", exc, exc_info=True
            )
            raise

        match_found = any(d.name == deployment_name for d in deployments)  # type: ignore[attr-defined]
        if match_found:
            SUMMER_PREFECT_LOGGER.info(
                "流部署 '%s' 已存在", deployment_name, extra={"info_color": "green"}
            )
        else:
            SUMMER_PREFECT_LOGGER.warning(
                "流部署 '%s' 不存在", deployment_name, extra={"info_color": "yellow"}
            )
        return match_found


def check_flow_deployment_sync(
    deployment_name: str,
    *,
    client_factory: Callable[[], object] = get_client,
) -> bool:
    """
    ``check_flow_deployment`` 的同步包装。
    """
    return asyncio.run(
        check_flow_deployment(deployment_name=deployment_name, client_factory=client_factory)
    )


__all__ = [
    "export_poetry_requirements_to_pips",
    "check_work_pool",
    "check_work_pool_sync",
    "check_flow_deployment",
    "check_flow_deployment_sync",
    "SUMMER_PREFECT_LOGGER",
]
