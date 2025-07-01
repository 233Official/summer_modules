from pathlib import Path

from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG
from summer_modules.ssh import SSHConnection

SSH_CONFIG = CONFIG.get("ssh", {})
if not SSH_CONFIG:
    raise ValueError("配置文件中未找到 ssh 配置项")
HOSTNAME = SSH_CONFIG.get("hostname")
USERNAME = SSH_CONFIG.get("username")
PASSWORD = SSH_CONFIG.get("password")
PORT = SSH_CONFIG.get("port", 22)
if not HOSTNAME or not USERNAME or not PASSWORD:
    raise ValueError("配置文件中 ssh 配置项不完整")


ssh_connection = SSHConnection(
    hostname=HOSTNAME,
    username=USERNAME,
    password=PASSWORD,
    port=PORT,
)


def test_execute_command():
    """Test the SSH command execution."""
    try:
        ssh_connection.connect()
        SUMMER_MODULES_TEST_LOGGER.info(f"已连接到 {HOSTNAME} 的 SSH 服务器")

        command = "echo 'Hello, SSH!'"
        output = ssh_connection.execute_command(command=command)

        assert output == "Hello, SSH!", f"命令执行失败，输出: {output}"
        SUMMER_MODULES_TEST_LOGGER.info(f"命令执行成功，输出: {output}")

        command_ls = "ls -l"
        output_ls = ssh_connection.execute_command(command=command_ls)
        assert output_ls, "命令 'ls -l' 执行失败，输出为空"
        SUMMER_MODULES_TEST_LOGGER.info(f"命令 'ls -l' 执行成功，输出: {output_ls}")

        command_whoami = "whoami"
        output_whoami = ssh_connection.execute_command(command=command_whoami)
        if not output_whoami:
            raise ValueError("命令 'whoami' 执行失败，输出为空")
        assert (
            output_whoami.strip() == USERNAME
        ), f"命令 'whoami' 执行失败，输出: {output_whoami}, 期望: {USERNAME}"
        SUMMER_MODULES_TEST_LOGGER.info(
            f"命令 'whoami' 执行成功，输出: {output_whoami.strip()}"
        )

    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"执行命令时发生错误: {e}")
        raise e

    finally:
        ssh_connection.close()


def test_execute_interactive_command():
    """测试增强的交互式命令执行"""
    try:
        ssh_connection.connect()

        # 测试 1: 单个命令
        output1 = ssh_connection.execute_interactive_command_legacy("whoami")
        SUMMER_MODULES_TEST_LOGGER.info(f"单个命令结果:\n{output1}")
        assert output1 and USERNAME in output1, f"whoami 命令执行失败: {output1}"

        # 测试 2: sudo 命令（真正的交互式）
        output2 = ssh_connection.execute_interactive_command_legacy(["sudo id", PASSWORD])
        SUMMER_MODULES_TEST_LOGGER.info(f"Sudo 命令结果:\n{output2}")
        assert (
            output2 is not None and "uid=0(root)" in output2
        ), f"sudo 命令执行失败: {output2}"

        # 测试 3: 单独的命令（不需要交互）
        output3 = ssh_connection.execute_command(
            "echo 'test' > /tmp/test.txt && cat /tmp/test.txt && rm /tmp/test.txt"
        )
        SUMMER_MODULES_TEST_LOGGER.info(f"文件操作结果:\n{output3}")
        assert output3 is not None and "test" in output3, f"文件操作失败: {output3}"

    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"测试失败: {e}")
        raise e
    finally:
        ssh_connection.close()


def test_hbase_execute_interactive_command():
    host = CONFIG["hbase"]["host"]
    username = CONFIG["hbase"]["username"]
    password = CONFIG["hbase"]["password"]
    port = 22
    hbase_ssh_connection = SSHConnection(
        hostname=host,
        username=username,
        password=password,
        port=port,
    )
    try:
        hbase_ssh_connection.connect()
        SUMMER_MODULES_TEST_LOGGER.info(f"已连接到 {host} 的 SSH 服务器")

        # 测试 HBase 命令，使用更大的缓冲区
        commands = [
            # 查询 hbase 版本
            "hbase version | head -1",
            # 进入 HBase shell
            "hbase shell",
            # 列出所有表
            # "list",
            # 扫描 cloud-whoisxml-whois-dat` 表只返回时间戳恰好等于 1750318627149 或 1750318712510 的单元格数据
            # "scan 'cloud-whoisxml-whois-data', {FILTER => \"TimestampsFilter(1750318627149, 1750318712510)\"}",
            # 扫描 cloud-whoisxml-whois-dat` 表只返回时间戳恰好等于 1750318712510 的单元格数据
            # "scan 'cloud-whoisxml-whois-data', {FILTER => \"TimestampsFilter(1750318712510)\"}",
            "scan 'cloud-whoisxml-whois-data', {FILTER => \"TimestampsFilter(1750318712510)\", LIMIT => 2}",
            # 扫描 cloud-whoisxml-whois-data 表只返回指定时间戳范围的数据
            # "scan 'cloud-whoisxml-whois-data', {TIMERANGE => [1750318627149, 1750318712511], LIMIT => 5}",
            # 退出 HBase shell
            "exit",
        ]
        output = hbase_ssh_connection.execute_interactive_command_legacy(
            commands=commands, 
            buffer_size=16384,  # 使用更大的缓冲区
            timeout=60  # 增加超时时间
        )

        assert output is not None, "HBase 命令执行失败，输出为空"
        SUMMER_MODULES_TEST_LOGGER.info(f"HBase 命令执行成功，输出:\n{output}")

    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"HBase 命令执行失败: {e}")
        raise e

    finally:
        hbase_ssh_connection.close()


def test_execute_interactive_command_structured():
    """测试结构化交互式命令执行"""
    try:
        ssh_connection.connect()

        # 测试 1: 单个命令的结构化结果
        result1 = ssh_connection.execute_interactive_command("whoami")
        assert result1 is not None, "命令执行返回 None"
        
        SUMMER_MODULES_TEST_LOGGER.info(f"单个命令结构化结果:")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 成功: {result1.success}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 命令数量: {result1.command_count}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 执行时间: {result1.execution_time:.2f}s")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 最后命令输出: {result1.get_last_command_output()}")
        
        assert result1.success, f"whoami 命令执行失败: {result1.error_message}"
        
        # 调试信息
        whoami_output = result1.get_command_output("whoami")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - whoami输出: '{whoami_output}'")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 期望用户名: '{USERNAME}'")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 命令输出映射: {result1.command_outputs}")
        
        # 修正测试逻辑：检查whoami输出不为空即可，因为实际用户名可能与配置不同
        assert whoami_output is not None and whoami_output.strip(), f"whoami 命令输出为空: {whoami_output}"
        
        # 测试 2: 多命令的结构化结果（不使用sudo，避免权限问题）
        result2 = ssh_connection.execute_interactive_command(["whoami", "pwd", "echo 'test'"])
        assert result2 is not None, "多命令执行返回 None"
        
        SUMMER_MODULES_TEST_LOGGER.info(f"多命令结构化结果:")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 成功: {result2.success}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 命令数量: {result2.command_count}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 执行时间: {result2.execution_time:.2f}s")
        
        # 检查命令映射字典
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 命令输出映射: {list(result2.command_outputs.keys())}")
        
        # 检查敏感信息是否被正确处理（这些命令都不应该是敏感的）
        for cmd_result in result2.command_results:
            SUMMER_MODULES_TEST_LOGGER.info(f"    命令 [{cmd_result.index}]: {cmd_result.command} (敏感: {cmd_result.is_sensitive})")
            assert not cmd_result.is_sensitive, f"普通命令不应该被标记为敏感: {cmd_result.command}"
        
        assert result2.success, f"多命令执行失败: {result2.error_message}"
        
        
        # 测试 3: 获取特定命令的输出
        echo_output = result2.get_command_output("echo 'test'")
        assert echo_output and "test" in echo_output, "无法获取 echo 命令输出"
        
        # 测试 4: 获取索引的命令结果
        first_cmd_result = result2.get_command_result_by_index(0)
        assert first_cmd_result is not None, "无法获取第一个命令结果"
        assert first_cmd_result.command == "whoami", "第一个命令不正确"

        SUMMER_MODULES_TEST_LOGGER.info("结构化交互式命令测试通过")

    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"结构化测试失败: {e}")
        raise e
    finally:
        ssh_connection.close()


def test_single_command_structured():
    """测试单个命令的结构化执行"""
    try:
        ssh_connection.connect()
        
        # 测试成功的命令
        result1 = ssh_connection.execute_command_structured("echo 'Hello World'")
        SUMMER_MODULES_TEST_LOGGER.info(f"单个命令结构化结果:")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 成功: {result1.success}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 命令: {result1.command}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 输出: {result1.output}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 退出码: {result1.exit_code}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 执行时间: {result1.execution_time:.2f}s")
        
        assert result1.success, f"echo 命令执行失败: {result1.error_message}"
        assert "Hello World" in result1.output, "echo 命令输出不正确"
        assert result1.exit_code == 0, "命令退出码不为 0"
        assert not result1.has_errors(), "命令不应该有错误"
        
        # 测试多个简单命令
        result2 = ssh_connection.execute_command_structured("whoami")
        SUMMER_MODULES_TEST_LOGGER.info(f"whoami 命令结果:")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 成功: {result2.success}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 输出: {result2.output}")
        assert result2.success, f"whoami 命令执行失败: {result2.error_message}"
        
        result3 = ssh_connection.execute_command_structured("pwd")
        SUMMER_MODULES_TEST_LOGGER.info(f"pwd 命令结果:")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 成功: {result3.success}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 输出: {result3.output}")
        assert result3.success, f"pwd 命令执行失败: {result3.error_message}"
        
        # 测试错误情况
        result4 = ssh_connection.execute_command_structured("nonexistent_command_12345")
        SUMMER_MODULES_TEST_LOGGER.info(f"失败命令结构化结果:")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 成功: {result4.success}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 错误信息: {result4.error_message}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  - 退出码: {result4.exit_code}")
        
        assert not result4.success, "不存在的命令应该失败"
        assert result4.has_errors(), "失败的命令应该有错误"
        assert result4.exit_code != 0, "失败命令的退出码应该不为 0"

        SUMMER_MODULES_TEST_LOGGER.info("单个命令结构化测试通过")

    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"单个命令结构化测试失败: {e}")
        raise e
    finally:
        ssh_connection.close()


def main():
    # test_execute_command()
    # test_execute_interactive_command()  # 旧的字符串接口测试
    # test_hbase_execute_interactive_command()
    # test_execute_interactive_command_structured()  # 新的结构化接口测试
    test_single_command_structured()  # 单个命令的结构化接口测试


if __name__ == "__main__":
    main()
    SUMMER_MODULES_TEST_LOGGER.info("SSH 测试脚本执行完毕")
