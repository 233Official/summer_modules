from pathlib import Path
from typing import Union, Optional, List
import paramiko
import time
import re
import traceback

from summer_modules.logger import init_and_get_logger
from summer_modules.ssh.ssh_model import (
    InteractiveCommandResult,
    CommandResult,
    SingleCommandResult,
)

CURRENT_DIR = Path(__file__).parent.resolve()
SSH_LOGGER = init_and_get_logger(current_dir=CURRENT_DIR, logger_name="ssh_logger")


class SSHConnection:
    def __init__(
        self, hostname: str, username: str, password: str, port: int = 22
    ) -> None:
        self.hostname = hostname
        """SSH 服务器的主机名或 IP 地址"""
        self.username = username
        """SSH 服务器的用户名"""
        self.password = password
        """SSH 服务器的密码"""
        self.port = port
        """SSH 服务器的端口，默认为 22"""
        self.client: Optional[paramiko.SSHClient] = None
        """Paramiko SSH 客户端实例"""
        self.invoke_shell: Optional[paramiko.Channel] = None
        """用于执行命令的交互式 shell"""
        self.hbase_shell: Optional[paramiko.Channel] = None
        """专门用于执行 Hbase 命令的交互式 shell"""
        SSH_LOGGER.info(f"SSH connection initialized for {self.hostname}")

    def connect(self, enbale_hbase_shell: bool = False) -> None:
        """建立 SSH 连接

        Args:
            enbale_hbase_shell: 是否初始化 HBase shell, 默认为 False
        """
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            hostname=self.hostname,
            username=self.username,
            password=self.password,
            port=self.port,
            look_for_keys=False,
            allow_agent=False,
        )
        SSH_LOGGER.info(f"已连接到 {self.hostname} 的 SSH 服务器")
        self.invoke_shell = self.client.invoke_shell()
        SSH_LOGGER.info(f"已初始化交互式 shell 用于执行命令集")
        # 如果需要 HBase shell，则初始化
        if enbale_hbase_shell:
            self.hbase_shell = self.client.invoke_shell()
            # self.hbase_shell 尝试进入 HBase shell 环境
            hbase_shell_init_result = self.execute_interactive_commands(
                commands=["hbase shell"],
                shell=self.hbase_shell,
            )
            if not hbase_shell_init_result or not hbase_shell_init_result.success:
                error_msg = (
                    "HBase shell 初始化失败，请检查 HBase 是否已正确安装并配置环境变量"
                )
                SSH_LOGGER.error(error_msg)
                raise RuntimeError(error_msg)
            SSH_LOGGER.info(f"HBase shell 已初始化用于执行 HBase 命令")

    def execute_command(self, command: str, timeout: int = 30) -> SingleCommandResult:
        """执行单个命令并返回结构化结果

        Args:
            command: 要执行的命令
            timeout: 命令执行超时时间，默认为 30 秒
        Returns:
            SingleCommandResult: 包含执行结果的结构化对象
        """
        start_time = time.time()

        if not self.client:
            error_msg = "SSH 连接未建立，请先调用 connect() 方法"
            SSH_LOGGER.error(error_msg)
            return SingleCommandResult(
                success=False,
                command=command,
                error_message=error_msg,
                execution_time=time.time() - start_time,
            )

        try:
            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            exit_status = stdout.channel.recv_exit_status()

            output = stdout.read().decode("utf-8")
            error = stderr.read().decode("utf-8")
            execution_time = time.time() - start_time

            if exit_status != 0:
                error_msg = f"命令执行失败 (退出码: {exit_status}): {error}"
                SSH_LOGGER.error(error_msg)
                return SingleCommandResult(
                    success=False,
                    command=command,
                    output=output.strip(),
                    exit_code=exit_status,
                    execution_time=execution_time,
                    error_message=error_msg,
                )

            SSH_LOGGER.info(
                f"已执行命令: {command} 在 {self.hostname} (用时: {execution_time:.2f}s)"
            )
            return SingleCommandResult(
                success=True,
                command=command,
                output=output.strip(),
                exit_code=exit_status,
                execution_time=execution_time,
            )

        except Exception as e:
            error_msg = f"执行命令失败: {e}"
            SSH_LOGGER.error(error_msg)
            execution_time = time.time() - start_time
            return SingleCommandResult(
                success=False,
                command=command,
                execution_time=execution_time,
                error_message=error_msg,
            )

    def execute_interactive_commands(
        self,
        commands: Union[str, list],
        timeout: int = 30,
        wait_for_ready: bool = True,
        wait_between_commands: float = 0.5,
        buffer_size: int = 1024,
        shell: Optional[paramiko.Channel] = None,
    ) -> Union[InteractiveCommandResult, None]:
        """执行交互式命令序列

        Args:
            commands: 要执行的命令列表或单个命令字符串
            timeout: 命令执行超时时间，默认为 30 秒
            wait_for_ready: 是否等待 shell 准备就绪，默认为 True
            wait_between_commands: 每个命令之间的等待时间，默认为 0.5 秒
            buffer_size: 数据读取缓冲区大小，默认为 1024 字节。
                        注意：由于底层SSH协议和paramiko库的限制，实际每次接收的数据量通常为1024字节，
                        设置更大的值目前不会提升性能，但保留此参数以备将来优化使用。
            shell: 可选的 paramiko Channel 对象，如果未提供则使用 invoke_shell
        Returns:
            InteractiveCommandResult: 包含完整执行结果的结构化对象，如果执行失败则返回 None
        """
        start_time = time.time()
        # 移除内部固定的 buffer_size 赋值，直接使用参数值
        if not shell:
            SSH_LOGGER.debug("未提供 shell, 使用 self.invoke_shell")
            shell = self.invoke_shell

        if not shell:
            error_msg = "SSH 连接未建立，请先调用 connect() 方法"
            SSH_LOGGER.error(error_msg)
            return InteractiveCommandResult(
                success=False,
                commands=[],
                error_message=error_msg,
                execution_time=time.time() - start_time,
            )

        # 标准化输入为列表
        if isinstance(commands, str):
            command_list = [commands]
        else:
            command_list = commands.copy()

        if not command_list:
            error_msg = "命令列表不能为空"
            SSH_LOGGER.error(error_msg)
            return InteractiveCommandResult(
                success=False,
                commands=[],
                error_message=error_msg,
                execution_time=time.time() - start_time,
            )

        # 等待 shell 准备就绪
        if wait_for_ready:
            self._wait_for_shell_ready(buffer_size=buffer_size)

        # 存储每个命令及其输出
        command_results = []
        current_command_index = 0
        current_output = ""
        command_output_started = False
        execution_successful = True
        error_message = None

        # 发送第一个命令
        first_command = command_list[0]
        shell.send(f"{first_command}\n".encode("utf-8"))

        # 安全日志记录：隐藏可能的密码
        safe_command = self._mask_sensitive_info(
            first_command, command_list, current_command_index
        )
        SSH_LOGGER.debug(f"发送命令 [{current_command_index}]: {safe_command}")

        def is_prompt_detected(text: str) -> bool:
            """检测是否出现了命令提示符"""
            if not text.strip():
                return False

            last_line = text.strip().split("\n")[-1]
            prompt_patterns = [
                r".*[$#>]\s*$",
                r".*[$#>]$",
                r".*:\s*[$#>]\s*$",
                r"\[.*\]\s*[$#>]\s*$",
                # HBase Shell 提示符模式
                r"hbase\(main\):\d+:\d+>\s*$",  # hbase(main):001:0>
                r"hbase\(main\):\d+:\d+>$",  # hbase(main):001:0>
                # sudo 命令执行后的提示符模式
                r".*@.*:.+[$#]\s*$",  # username@hostname:/path#
                r".*@.*:.+[$#]$",  # username@hostname:/path#
                r"root@.*:.+[$#]\s*$",  # root@hostname:/path#
                r"root@.*:.+[$#]$",  # root@hostname:/path#
            ]

            for pattern in prompt_patterns:
                if re.match(pattern, last_line):
                    SSH_LOGGER.debug(
                        f"检测到提示符模式: {pattern} in line: {last_line}"
                    )
                    return True

            # 修改 sudo 命令特殊处理：更精确的检测
            # 排除明显不是提示符的内容（如 URL、长文本等）
            if (
                "http://" not in last_line  # 排除 URL
                and "https://" not in last_line
                and len(last_line) < 100  # 排除过长的文本
                and not last_line.startswith("SLF4J:")  # 排除 SLF4J 日志
                and not "Class path" in last_line  # 排除类路径相关信息
                and any(indicator in last_line for indicator in ["$", "#", ">", "@"])
                and (":" in last_line or "/" in last_line)
            ):
                SSH_LOGGER.debug(f"检测到可能的 sudo 后提示符: {last_line}")
                return True

            return False

        def is_waiting_for_input(text: str) -> bool:
            """检测是否在等待用户输入"""
            last_line = text.strip().split("\n")[-1] if text.strip() else ""

            # 常见的等待输入模式
            input_patterns = [
                r".*password.*:",  # 密码提示
                r".*Password.*:",  # 密码提示（大写）
                r".*\[sudo\].*:",  # sudo 密码提示
                r".*\(y/n\).*",  # 确认提示
                r".*\[Y/n\].*",  # 确认提示
                r".*Enter.*:",  # 输入提示
            ]

            for pattern in input_patterns:
                if re.search(pattern, last_line, re.IGNORECASE):
                    SSH_LOGGER.debug(f"检测到输入等待模式: {pattern}")
                    return True
            return False

        # 主循环
        try:
            while True:
                # 对于 sudo 命令特殊处理
                sudo_command_in_progress = False
                if current_command_index > 0 and current_command_index < len(
                    command_list
                ):
                    if (
                        command_list[current_command_index - 1]
                        .strip()
                        .startswith("sudo ")
                    ):
                        sudo_command_in_progress = True

                # 超时检查，为 sudo 命令提供更宽松的超时处理
                current_timeout = timeout * 2 if sudo_command_in_progress else timeout
                if time.time() - start_time > current_timeout:
                    error_message = (
                        f"命令执行超时，当前执行到第 {current_command_index + 1} 个命令"
                    )
                    if sudo_command_in_progress:
                        error_message += "(sudo 命令)"
                    SSH_LOGGER.warning(error_message)
                    execution_successful = False
                    break

                if shell.recv_ready():
                    # 连续读取所有可用数据，不只是单次 recv
                    chunk = ""
                    try:
                        # 第一次读取
                        data = shell.recv(buffer_size)
                        if data:
                            chunk += data.decode("utf-8", errors="ignore")

                            # 继续读取剩余数据，直到没有更多数据可读
                            while shell.recv_ready():
                                additional_data = shell.recv(buffer_size)
                                if not additional_data:
                                    break
                                chunk += additional_data.decode(
                                    "utf-8", errors="ignore"
                                )

                    except UnicodeDecodeError:
                        SSH_LOGGER.warning("数据解码错误，跳过此数据块")
                        continue

                    if not chunk:  # 连接可能已断开
                        error_message = "接收到空数据，连接可能已断开"
                        SSH_LOGGER.warning(error_message)
                        execution_successful = False
                        break

                    # 开始记录输出的逻辑
                    if not command_output_started:
                        if first_command.strip() in chunk:
                            command_output_started = True
                            cmd_index = chunk.find(first_command.strip())
                            if cmd_index != -1:
                                chunk = chunk[cmd_index:]
                        else:
                            continue

                    current_output += chunk
                    # SSH_LOGGER.debug(f"接收到数据块大小: {len(chunk)} 字节")
                    SSH_LOGGER.debug(f"详细内容: {chunk.strip()}")

                    # 检查是否需要发送下一个命令
                    if current_command_index < len(command_list) - 1:
                        # 检查是否等待输入或提示符就绪
                        should_send_next = is_waiting_for_input(
                            current_output
                        ) or is_prompt_detected(current_output)

                        # 特殊处理 sudo 命令：检查是否为密码输入后的结果
                        if not should_send_next and current_command_index > 0:
                            prev_cmd = command_list[current_command_index - 1]
                            current_cmd = command_list[current_command_index]

                            # 如果前一个命令是 sudo，当前是密码，并且有一定时间过去了
                            if (
                                prev_cmd.startswith("sudo ")
                                and " " not in current_cmd.strip()
                            ):
                                # 等待足够时间确保命令已经执行
                                if (
                                    time.time() - start_time > 1.0
                                    and len(current_output) > 0
                                ):
                                    # 检查输出中是否有表示命令执行结束的内容
                                    output_lines = current_output.strip().split("\n")
                                    if len(
                                        output_lines
                                    ) >= 2 and not current_output.endswith(
                                        "password for"
                                    ):
                                        SSH_LOGGER.debug(
                                            f"sudo 密码命令似乎已完成，检测到 {len(output_lines)} 行输出"
                                        )
                                        should_send_next = True

                        if should_send_next:
                            # 保存当前命令的输出
                            current_cmd = command_list[current_command_index]
                            cleaned_output = self._clean_command_output(
                                current_output, current_cmd
                            )

                            # 检查是否为敏感命令
                            is_sensitive = self._is_sensitive_command(
                                current_cmd, command_list, current_command_index
                            )

                            command_results.append(
                                CommandResult(
                                    command=current_cmd,
                                    output=cleaned_output,
                                    index=current_command_index,
                                    is_sensitive=is_sensitive,
                                )
                            )

                            # 准备下一个命令
                            current_command_index += 1
                            next_command = command_list[current_command_index]
                            current_output = ""  # 重置输出缓冲区

                            # 等待一小段时间确保提示完全显示
                            time.sleep(wait_between_commands)

                            shell.send(f"{next_command}\n".encode("utf-8"))

                            # 安全日志记录
                            safe_next_command = self._mask_sensitive_info(
                                next_command, command_list, current_command_index
                            )
                            SSH_LOGGER.debug(
                                f"发送命令 [{current_command_index}]: {safe_next_command}"
                            )
                            continue

                    # 如果所有命令都已发送，检查是否完成
                    if current_command_index >= len(command_list) - 1:
                        if is_prompt_detected(current_output):
                            # 保存最后一个命令的输出
                            last_cmd = command_list[current_command_index]
                            cleaned_output = self._clean_command_output(
                                current_output, last_cmd
                            )

                            # 检查是否为敏感命令
                            is_sensitive = self._is_sensitive_command(
                                last_cmd, command_list, current_command_index
                            )

                            command_results.append(
                                CommandResult(
                                    command=last_cmd,
                                    output=cleaned_output,
                                    index=current_command_index,
                                    is_sensitive=is_sensitive,
                                )
                            )
                            SSH_LOGGER.debug("所有命令执行完成")
                            break

                else:
                    SSH_LOGGER.debug(
                        "当前没有可读取的数据，可能是命令正在执行中或等待输入"
                    )
                    time.sleep(1)

        except Exception as e:
            error_message = f"命令执行过程中发生异常: {e}"
            SSH_LOGGER.error(error_message)
            execution_successful = False

        # 构建结果对象
        execution_time = time.time() - start_time

        # 创建命令到输出的映射字典
        command_outputs = {}
        for result in command_results:
            # 对于敏感命令，不在字典中暴露其内容
            if result.is_sensitive:
                command_outputs[result.command] = "***"
            else:
                command_outputs[result.command] = result.output

        # 格式化最终输出
        formatted_output = self._format_command_results(command_results)

        # 安全日志记录
        safe_command_summary = " -> ".join(
            [
                self._mask_sensitive_info(cmd, command_list, i)
                for i, cmd in enumerate(command_list)
            ]
        )
        SSH_LOGGER.info(
            f"已执行交互命令序列: {safe_command_summary} 在 {self.hostname} (用时: {execution_time:.2f}s)"
        )

        return InteractiveCommandResult(
            success=execution_successful,
            commands=command_list,
            command_results=command_results,
            command_outputs=command_outputs,
            formatted_output=formatted_output,
            execution_time=execution_time,
            error_message=error_message,
        )

    def execute_hbase_command(
        self, command: str, timeout: int = 300
    ) -> SingleCommandResult:  # type: ignore
        """执行 HBase shell 命令并返回结构化结果s
        由于 Hbase 命令需要先进入 HBase shell 环境，然后一直在 HBase shell 中执行命令，所以不能采用 execute_command 这样的单条命令执行输出与清空的机制

        Args:
            command: 要执行的 HBase shell 命令
            timeout: 命令执行超时时间，默认为 300 秒
        Returns:
            SingleCommandResult: 包含执行结果的结构化对象
        """
        # 检查 HBase shell 是否已初始化
        if not self.hbase_shell:
            error_msg = "HBase shell 未初始化，请先调用 connect() 方法"
            SSH_LOGGER.error(error_msg)
            return SingleCommandResult(
                success=False,
                command=command,
                error_message=error_msg,
                execution_time=0,
            )

        start_time = time.time()

        # 调用 execute_interactive_commands 方法执行 Hbase shell 命令存在无法解决的严重问题, 放弃使用, 这里仅做保留归档
        # # 调用 execute_interactive_commands 方法执行 HBase shell 命令
        # result = self.execute_interactive_commands(
        #     commands=[command],
        #     timeout=timeout,
        #     wait_for_ready=True,
        #     shell=self.hbase_shell,  # 使用 HBase shell
        # )
        # if not result or not result.success:
        #     error_msg = f"HBase shell 命令执行失败: {result.error_message if result else '未知错误'}"
        #     SSH_LOGGER.error(error_msg)
        #     return SingleCommandResult(
        #         success=False,
        #         command=command,
        #         error_message=error_msg,
        #         execution_time=time.time() - start_time,
        #     )
        # 如果执行成功，返回结果
        # SSH_LOGGER.info(
        #     f"已执行 HBase shell 命令: {command} 在 {self.hostname} (用时: {result.execution_time:.2f}s)"
        # )

        # 直接使用 self.hbase_shell 执行命令
        self.hbase_shell.send(f"{command}\n".encode("utf-8"))
        SSH_LOGGER.debug(f"发送 HBase shell 命令: {command}")

        # 等待命令执行完成
        output = ""
        while True:
            if self.hbase_shell.recv_ready():
                chunk = self.hbase_shell.recv(1024).decode("utf-8", errors="ignore")
                output += chunk
                SSH_LOGGER.debug(f"接收到 HBase shell 输出: {chunk.strip()}")
            else:
                # 检查是否有类似 hbase(main):001:0> 的提示符
                if re.search(r"hbase\(main\):\d+:\d+>\s*$", output):
                    SSH_LOGGER.debug("检测到 HBase shell 提示符，命令执行完成")
                    break
            time.sleep(0.1)

        execution_time = time.time() - start_time
        SSH_LOGGER.debug(
            f"已执行 HBase shell 命令: {command}，输出: {output.strip()}, 用时: {execution_time:.2f}s"
        )

        return SingleCommandResult(
            success=True,
            command=command,
            output=output.strip(),
            exit_code=0,  # HBase shell 命令通常没有退出码
            execution_time=execution_time,
        )

    def _mask_sensitive_info(
        self, text: str, command_context: Optional[list] = None, current_index: int = -1
    ) -> str:
        """隐藏敏感信息，如密码

        Args:
            text: 要检查的文本
            command_context: 完整的命令序列上下文
            current_index: 当前命令在序列中的索引
        """
        # 检查是否是 sudo 命令后的密码输入
        if command_context and current_index > 0:
            prev_command = command_context[current_index - 1].strip().lower()
            current_text = text.strip()

            # 如果前一个命令包含 sudo，且当前是简单的字符串（可能是密码）
            if (
                prev_command.startswith("sudo ")
                and current_text
                and " " not in current_text
                and len(current_text) > 0
            ):
                return "***"

        # 其他情况都不掩码
        return text

    def _clean_command_output(self, output: str, command: str) -> str:
        """清理单个命令的输出"""
        if not output:
            return ""

        lines = output.split("\n")
        result_lines = []

        # 需要跳过的模式
        skip_patterns = [
            r".*[$#>]\s*$",  # 提示符
            r".*\[sudo\] password.*",  # sudo 密码提示
            r".*password.*:.*",  # 一般密码提示
            r".*Password.*:.*",  # 密码提示
            r"^\s*$",  # 空行
            r"hbase\(main\):\d+:\d+>\s*$",  # HBase shell 提示符
            r"hbase\(main\):\d+:\d+>$",  # HBase shell 提示符
            r".*@.*:.+[$#]\s*$",  # sudo后的提示符
            r".*@.*:.+[$#]$",  # sudo后的提示符
        ]

        # 添加当前命令到跳过列表（但不包括可能的密码）
        if command.strip() and " " in command:  # 只跳过包含空格的命令行
            skip_patterns.append(re.escape(command.strip()))

        # 对于密码输入，特殊处理
        if command.strip() and " " not in command.strip():
            # 可能是密码，移除echo关闭和密码相关提示
            lines = [
                l for l in lines if not any(x in l for x in ["password", "[sudo]"])
            ]

        for line in lines:
            should_skip = False

            for pattern in skip_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    should_skip = True
                    break

            if not should_skip and line.strip():
                result_lines.append(line.strip())

        return "\n".join(result_lines)

    def _format_command_results(self, command_results: List[CommandResult]) -> str:
        """格式化命令执行结果，为每个命令添加清晰的分割"""
        if not command_results:
            return ""

        formatted_output = []
        separator = "=" * 60

        for i, result in enumerate(command_results):
            # 添加命令标题
            safe_command = self._mask_sensitive_info(
                result.command, [r.command for r in command_results], result.index
            )
            formatted_output.append(f"执行命令 [{result.index + 1}]: {safe_command}")
            formatted_output.append("-" * 40)

            # 添加命令输出
            if result.output.strip():
                # 敏感命令需要隐藏, 但是输出无需隐藏
                formatted_output.append(result.output)
            else:
                formatted_output.append("(无输出)")

            # 添加分割线（除了最后一个命令）
            if i < len(command_results) - 1:
                formatted_output.append("")
                formatted_output.append(separator)
                formatted_output.append("")

        return "\n".join(formatted_output)

    def _wait_for_shell_ready(
        self,
        timeout: int = 5,
        buffer_size: int = 8192,
        shell: Optional[paramiko.Channel] = None,
    ) -> None:
        """等待 shell 准备就绪并清空初始输出"""
        if not shell:
            shell = self.invoke_shell
        start_time = time.time()
        if not shell:
            SSH_LOGGER.error("SSH 连接未建立，请先调用 connect() 方法")
            return

        while time.time() - start_time < timeout:
            if shell.recv_ready():
                # 读取并丢弃初始输出
                shell.recv(buffer_size)
            else:
                # 发送一个简单命令来确认 shell 准备就绪
                shell.send("echo ready\n".encode("utf-8"))
                time.sleep(0.5)

                # 检查是否收到回应
                ready_output = ""
                while shell.recv_ready():
                    ready_output += shell.recv(buffer_size).decode("utf-8")
                    SSH_LOGGER.debug(f"接收到初始输出: {ready_output.strip()}")

                if "ready" in ready_output:
                    SSH_LOGGER.debug("Shell 已准备就绪")
                    break

            time.sleep(0.1)

    def _is_sensitive_command(
        self,
        command: str,
        command_context: Optional[list] = None,
        current_index: int = -1,
    ) -> bool:
        """检测是否为敏感命令（如密码输入）

        设计思路：只有 sudo xxx 后的命令必定是 password，password 需要掩码，其他情况都不需要掩码

        Args:
            command: 要检查的命令
            command_context: 完整的命令序列上下文
            current_index: 当前命令在序列中的索引
        Returns:
            bool: 如果是敏感命令返回 True，否则返回 False
        """
        # 只有一种情况需要掩码：sudo 命令后的密码输入
        if command_context and current_index > 0:
            prev_command = command_context[current_index - 1].strip().lower()

            # 如果前一个命令是 sudo 开头的命令，当前命令就是密码
            if prev_command.startswith("sudo "):
                return True

        # 其他所有情况都不需要掩码
        return False

    def close(self) -> None:
        """关闭 SSH 连接和所有相关的 Channel"""
        # 首先关闭 HBase shell channel
        if self.hbase_shell:
            try:
                self.hbase_shell.close()
                SSH_LOGGER.debug("已关闭 HBase shell channel")
            except Exception as e:
                SSH_LOGGER.warning(f"关闭 HBase shell channel 时出错: {e}")
            finally:
                self.hbase_shell = None

        # 关闭交互式 shell channel
        if self.invoke_shell:
            try:
                self.invoke_shell.close()
                SSH_LOGGER.debug("已关闭交互式 shell channel")
            except Exception as e:
                SSH_LOGGER.warning(f"关闭交互式 shell channel 时出错: {e}")
            finally:
                self.invoke_shell = None

        # 最后关闭 SSH 客户端连接
        if self.client:
            try:
                self.client.close()
                SSH_LOGGER.info(f"已关闭到 {self.hostname} 的 SSH 连接")
            except Exception as e:
                SSH_LOGGER.error(f"关闭 SSH 连接时出错: {e}")
            finally:
                self.client = None
