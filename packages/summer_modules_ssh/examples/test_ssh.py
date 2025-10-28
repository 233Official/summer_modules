"""示例：展示 `SSHConnection` 的常见用法。

运行前准备 config.toml：

    [ssh]
    hostname = "example.com"
    username = "user"
    password = "pass"
    port = 22

可选的 HBase 配置（需要具备 HBase 环境才可运行相关示例）：

    [hbase]
    host = "10.0.0.1"
    username = "hbase_user"
    password = "hbase_pass"

运行方式：

    uv run python -m packages.summer_modules_ssh.examples.test_ssh
"""

import traceback
from typing import Any, Dict, Iterable, Mapping

from summer_modules_core.logger import init_and_get_logger

from summer_modules_ssh import SSHConnection

from . import EXAMPLES_ROOT, get_hbase_config, get_ssh_config

LOGGER = init_and_get_logger(EXAMPLES_ROOT, "ssh_example")


def _ensure_required(section: str, config: Mapping[str, Any], required: Iterable[str]) -> Dict[str, Any]:
    data: Dict[str, Any] = dict(config)
    missing = [key for key in required if not data.get(key)]
    if missing:
        raise RuntimeError(f"[{section}] 配置缺少字段: {', '.join(missing)}")
    return data


def _get_int(config: Mapping[str, Any], key: str, default: int) -> int:
    value = config.get(key, default)
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:  # pragma: no cover - 配置错误时抛出
            raise RuntimeError(f"[{key}] = {value!r} 无法转换为整数") from exc

    raise RuntimeError(f"[{key}] 类型 {type(value)!r} 无法转换为整数")


def run_basic_commands(ssh: SSHConnection) -> None:
    LOGGER.info("=== 演示：执行单个命令 ===")
    hello = ssh.execute_command("echo 'Hello World'")
    LOGGER.info("echo -> success=%s, output=%s", hello.success, hello.output.strip())
    whoami = ssh.execute_command("whoami")
    LOGGER.info("whoami -> %s", whoami.output.strip())
    pwd = ssh.execute_command("pwd")
    LOGGER.info("pwd -> %s", pwd.output.strip())

    failure = ssh.execute_command("nonexistent_command_12345")
    LOGGER.info(
        "nonexistent_command_12345 -> success=%s, exit_code=%s, error=%s",
        failure.success,
        failure.exit_code,
        failure.error_message,
    )


def run_interactive_demo(ssh: SSHConnection) -> None:
    LOGGER.info("=== 演示：交互式命令序列 ===")
    result = ssh.execute_interactive_commands(["whoami", "pwd", "ls -1 | head -3"])
    if result is None or not result.success:
        LOGGER.error("交互式命令执行失败: %s", result.error_message if result else "未知错误")
        return

    LOGGER.info("命令数量: %s", result.command_count)
    for command, output in result.command_outputs.items():
        LOGGER.info("--- %s ---", command)
        LOGGER.info("%s", output)


def run_hbase_interactive_demo(hbase_cfg: Dict[str, Any]) -> None:
    LOGGER.info("=== 演示：使用 SSH 执行 HBase 命令 ===")
    ssh = SSHConnection(
        hostname=str(hbase_cfg["host"]),
        username=str(hbase_cfg["username"]),
        password=str(hbase_cfg["password"]),
        port=_get_int(hbase_cfg, "port", 22),
    )
    try:
        ssh.connect()
        commands = [
            "hbase version | head -1",
            "hbase shell",
            "list",
            "exit",
        ]
        result = ssh.execute_interactive_commands(commands, timeout=60)
        if result is None or not result.success:
            LOGGER.error("HBase 命令执行失败: %s", result.error_message if result else "未知错误")
            return

        LOGGER.info("HBase 命令执行完成，输出如下：")
        LOGGER.info("%s", result.formatted_output)
    finally:
        ssh.close()


def run_hbase_shell_demo(hbase_cfg: Dict[str, Any]) -> None:
    LOGGER.info("=== 演示：进入 HBase Shell 并执行命令 ===")
    ssh = SSHConnection(
        hostname=str(hbase_cfg["host"]),
        username=str(hbase_cfg["username"]),
        password=str(hbase_cfg["password"]),
        port=_get_int(hbase_cfg, "port", 22),
    )
    try:
        ssh.connect(enable_hbase_shell=True, terminal_width=200, terminal_height=60)
        scan_result = ssh.execute_hbase_command(
            "scan 'cloud-whoisxml-whois-data', {LIMIT => 2}", timeout=10
        )
        LOGGER.info("HBase scan -> success=%s", scan_result.success)
        LOGGER.info("输出：\n%s", scan_result.output)
    finally:
        ssh.close()


def main() -> None:
    ssh_cfg = _ensure_required("ssh", get_ssh_config(), ("hostname", "username", "password"))
    ssh = SSHConnection(
        hostname=str(ssh_cfg["hostname"]),
        username=str(ssh_cfg["username"]),
        password=str(ssh_cfg["password"]),
        port=_get_int(ssh_cfg, "port", 22),
    )

    LOGGER.info("=== 开始 SSH 示例 ===")
    try:
        ssh.connect()
        run_basic_commands(ssh)
        run_interactive_demo(ssh)
    finally:
        ssh.close()
        LOGGER.info("SSH 连接已关闭")

    hbase_cfg = get_hbase_config()
    if hbase_cfg:
        try:
            _ensure_required("hbase", hbase_cfg, ("host", "username", "password"))
        except RuntimeError as exc:
            LOGGER.warning("HBase 示例跳过：%s", exc)
            return

        try:
            run_hbase_interactive_demo(hbase_cfg)
            run_hbase_shell_demo(hbase_cfg)
        except Exception as exc:  # pragma: no cover - 示例代码记录异常即可
            LOGGER.error("运行 HBase 示例时出现异常: %s", exc)
            LOGGER.debug("堆栈跟踪:\n%s", traceback.format_exc())
    else:
        LOGGER.info("未找到 [hbase] 配置，跳过 HBase 示例。")


if __name__ == "__main__":
    main()
