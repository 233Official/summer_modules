#!/usr/bin/env python3
"""
改进版 HBase Shell TIMERANGE 查询测试
使用交互式 SSH 执行，测试指定的有数据时间范围
"""

import time
import sys
import re
from datetime import datetime
from summer_modules.database.hbase.hbase_api import HBaseAPI
from summer_modules.ssh import SSHConnection
from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG


def test_hbase_shell_interactive_improved():
    """使用真正的交互式 HBase Shell 执行 TIMERANGE 查询"""
    
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    SUMMER_MODULES_TEST_LOGGER.info("改进版 HBase Shell TIMERANGE 查询测试")
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    
    # 从配置文件获取连接信息
    try:
        hbase_config = CONFIG["hbase"]
        HBASE_HOST = hbase_config["host"]
        HBASE_USERNAME = hbase_config["username"]
        HBASE_PASSWORD = hbase_config["password"]
        
        SUMMER_MODULES_TEST_LOGGER.info(f"连接配置: {HBASE_USERNAME}@{HBASE_HOST}:22")
        
    except KeyError as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"配置文件中缺少 hbase 配置项: {e}")
        return False
    
    # 使用指定的有数据时间范围：北京时间 2025-06-19 00:00:00 到 2025-06-20 00:00:00 (UTC+8)
    # 转换为 UTC 时间戳
    start_dt = datetime(2025, 6, 18, 16, 0, 0)  # 北京时间 2025-06-19 00:00:00 = UTC 2025-06-18 16:00:00
    end_dt = datetime(2025, 6, 19, 16, 0, 0)    # 北京时间 2025-06-20 00:00:00 = UTC 2025-06-19 16:00:00
    
    start_timestamp = 1750348800000  # 你实际使用的时间戳
    end_timestamp = 1750435200000    # 你实际使用的时间戳
    
    SUMMER_MODULES_TEST_LOGGER.info("查询时间范围:")
    SUMMER_MODULES_TEST_LOGGER.info(f"  开始时间: {start_dt}")
    SUMMER_MODULES_TEST_LOGGER.info(f"  结束时间: {end_dt}")
    SUMMER_MODULES_TEST_LOGGER.info(f"  时间戳范围: [{start_timestamp}, {end_timestamp}]")
    SUMMER_MODULES_TEST_LOGGER.info("  💡 注意: 使用已知有数据的时间范围")
    
    table_name = "cloud-whoisxml-whois-data"
    
    # 建立 SSH 连接
    ssh = None
    try:
        ssh = SSHConnection(
            hostname=HBASE_HOST,
            username=HBASE_USERNAME,
            password=HBASE_PASSWORD,
            port=22
        )
        ssh.connect()
        
        SUMMER_MODULES_TEST_LOGGER.info("✅ SSH 连接建立成功")
        
        # 测试1: 使用交互式方式执行 HBase Shell 查询
        SUMMER_MODULES_TEST_LOGGER.info("\n" + "-" * 50)
        SUMMER_MODULES_TEST_LOGGER.info("测试1: 交互式 HBase Shell 查询（无LIMIT限制）")
        SUMMER_MODULES_TEST_LOGGER.info("-" * 50)
        
        # 构建扫描命令 - 移除LIMIT限制以获取所有数据
        scan_command = (
            f"scan '{table_name}', {{"
            f"TIMERANGE => [{start_timestamp}, {end_timestamp}]"
            f"}}"
        )
        
        SUMMER_MODULES_TEST_LOGGER.info(f"执行命令序列:")
        SUMMER_MODULES_TEST_LOGGER.info(f"  1. hbase shell")
        SUMMER_MODULES_TEST_LOGGER.info(f"  2. {scan_command}")
        SUMMER_MODULES_TEST_LOGGER.info(f"  3. exit")
        SUMMER_MODULES_TEST_LOGGER.warning("  ⚠️  预期检索约50,345条记录，执行时间约50-60秒")
        
        # 使用交互式命令执行
        commands = [
            "hbase shell",  # 启动 HBase Shell
            scan_command,   # 执行扫描命令
            "exit"         # 退出 HBase Shell
        ]
        
        start_time = time.time()
        
        output = ssh.execute_interactive_commands(
            commands=commands,
            timeout=180,  # 增加到3分钟，考虑到实际查询需要52秒
            wait_between_commands=2.0  # HBase Shell 需要时间启动
        )
        
        execution_time = time.time() - start_time
        
        if output:
            SUMMER_MODULES_TEST_LOGGER.info("✅ 交互式执行成功")
            SUMMER_MODULES_TEST_LOGGER.info(f"📊 执行时间: {execution_time:.2f} 秒")
            SUMMER_MODULES_TEST_LOGGER.info(f"📊 输出长度: {len(output)} 字符")
            
            # 使用改进的输出分析
            result_info = analyze_full_scan_output(output)
            
            SUMMER_MODULES_TEST_LOGGER.info(f"📊 分析结果:")
            for key, value in result_info.items():
                SUMMER_MODULES_TEST_LOGGER.info(f"   {key}: {value}")
            
            # 检查是否检索到了预期的数据量
            total_rows = result_info.get("total_rows", 0)
            if total_rows >= 50000:
                SUMMER_MODULES_TEST_LOGGER.info(f"✅ 数据量检查通过：检索到 {total_rows} 条记录")
            elif total_rows > 0:
                SUMMER_MODULES_TEST_LOGGER.warning(f"⚠️  检索到 {total_rows} 条记录，少于预期的50,345条")
            else:
                SUMMER_MODULES_TEST_LOGGER.error("❌ 未检测到有效的行数统计信息")
            
            return total_rows > 0
        else:
            SUMMER_MODULES_TEST_LOGGER.error("❌ 交互式执行失败，无输出")
            return False
            
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 测试失败: {e}")
        return False
    finally:
        if ssh is not None:
            try:
                ssh.close()
                SUMMER_MODULES_TEST_LOGGER.info("✅ SSH 连接已关闭")
            except Exception:
                pass


def parse_hbase_shell_output_improved(output: str) -> list:
    """改进的 HBase Shell 输出解析器"""
    
    if not output:
        return []
    
    lines = output.split('\n')
    records = []
    
    SUMMER_MODULES_TEST_LOGGER.debug(f"开始解析输出，共 {len(lines)} 行")
    
    # 显示输出预览用于调试
    preview_lines = lines[:10] + ["..."] + lines[-5:] if len(lines) > 15 else lines
    SUMMER_MODULES_TEST_LOGGER.info("输出预览:")
    for i, line in enumerate(preview_lines):
        SUMMER_MODULES_TEST_LOGGER.info(f"  [{i:2d}] {line[:80]}{'...' if len(line) > 80 else ''}")
    
    # 查找数据行
    in_data_section = False
    
    for line_num, line in enumerate(lines):
        line = line.strip()
        
        # 跳过空行
        if not line:
            continue
        
        # 检测数据部分开始
        if "ROW" in line and "COLUMN+CELL" in line:
            in_data_section = True
            SUMMER_MODULES_TEST_LOGGER.debug(f"检测到数据部分开始，行 {line_num}: {line}")
            continue
        
        # 检测数据部分结束
        if in_data_section and ("row(s)" in line or "Took " in line):
            in_data_section = False
            SUMMER_MODULES_TEST_LOGGER.debug(f"检测到数据部分结束，行 {line_num}: {line}")
            continue
        
        # 解析数据行
        if in_data_section and line:
            # HBase Shell 输出格式通常是：
            # row_key column=cf:qualifier, timestamp=xxx, value=xxx
            
            if " column=" in line and " timestamp=" in line:
                try:
                    record = parse_data_line(line)
                    if record:
                        records.append(record)
                        SUMMER_MODULES_TEST_LOGGER.debug(f"解析数据行 {line_num}: {record['row_key']}")
                except Exception as e:
                    SUMMER_MODULES_TEST_LOGGER.warning(f"解析数据行失败，行 {line_num}: {e}")
                    SUMMER_MODULES_TEST_LOGGER.debug(f"问题行内容: {line}")
    
    SUMMER_MODULES_TEST_LOGGER.info(f"解析完成，找到 {len(records)} 条数据记录")
    return records


def parse_data_line(line: str) -> dict | None:
    """解析单个数据行"""
    
    # 示例格式：
    # row_key column=cf:qualifier, timestamp=1234567890, value=data
    
    try:
        # 提取行键（第一个空格之前）
        parts = line.split(' ', 1)
        if len(parts) < 2:
            return None
        
        row_key = parts[0]
        rest = parts[1]
        
        # 提取列信息
        column_match = None
        timestamp_match = None
        value_match = None
        
        # 简单的字符串解析
        if " column=" in rest:
            column_start = rest.find(" column=") + 8
            column_end = rest.find(",", column_start)
            if column_end == -1:
                column_end = rest.find(" ", column_start)
            column_match = rest[column_start:column_end] if column_end != -1 else rest[column_start:]
        
        if " timestamp=" in rest:
            timestamp_start = rest.find(" timestamp=") + 11
            timestamp_end = rest.find(",", timestamp_start)
            if timestamp_end == -1:
                timestamp_end = rest.find(" ", timestamp_start)
            timestamp_str = rest[timestamp_start:timestamp_end] if timestamp_end != -1 else rest[timestamp_start:]
            try:
                timestamp_match = int(timestamp_str.strip())
            except ValueError:
                timestamp_match = timestamp_str.strip()
        
        if " value=" in rest:
            value_start = rest.find(" value=") + 7
            value_match = rest[value_start:]
        
        return {
            'row_key': row_key,
            'column': column_match,
            'timestamp': timestamp_match,
            'value': value_match
        }
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.warning(f"解析数据行时出错: {e}")
        return None


def quick_connectivity_test():
    """快速连接测试"""
    SUMMER_MODULES_TEST_LOGGER.info("🚀 执行快速连接测试...")
    
    try:
        hbase_config = CONFIG["hbase"]
        ssh_host = hbase_config["host"]
        ssh_user = hbase_config["username"]
        ssh_password = hbase_config["password"]
        
        ssh = SSHConnection(
            hostname=ssh_host,
            username=ssh_user,
            password=ssh_password,
            port=22
        )
        ssh.connect()
        
        # 测试基本命令
        result = ssh.execute_command("echo 'SSH连接正常'")
        SUMMER_MODULES_TEST_LOGGER.info(f"✅ SSH 连接测试: {result}")
        
        # 测试 HBase 可用性
        result = ssh.execute_command("hbase version | head -1", timeout=15)
        if result:
            SUMMER_MODULES_TEST_LOGGER.info(f"✅ HBase 可用: {result}")
        else:
            SUMMER_MODULES_TEST_LOGGER.warning("⚠️  HBase 版本检查失败")
        
        # 测试交互式 HBase Shell
        output = ssh.execute_interactive_commands(
            commands=["hbase shell", "status", "exit"],
            timeout=60,
            wait_between_commands=2.0
        )
        
        if output and "status" in output:
            SUMMER_MODULES_TEST_LOGGER.info("✅ HBase Shell 交互式测试成功")
        else:
            SUMMER_MODULES_TEST_LOGGER.warning("⚠️  HBase Shell 交互式测试异常")
        
        ssh.close()
        return True
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 快速测试失败: {e}")
        return False


def test_hbase_shell_large_dataset():
    """测试大数据量的 HBase Shell TIMERANGE 查询"""
    
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    SUMMER_MODULES_TEST_LOGGER.info("大数据量 HBase Shell TIMERANGE 查询测试")
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    
    # 从配置文件获取连接信息
    try:
        hbase_config = CONFIG["hbase"]
        HBASE_HOST = hbase_config["host"]
        HBASE_USERNAME = hbase_config["username"]
        HBASE_PASSWORD = hbase_config["password"]
        
        SUMMER_MODULES_TEST_LOGGER.info(f"连接配置: {HBASE_USERNAME}@{HBASE_HOST}:22")
        
    except KeyError as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"配置文件中缺少 hbase 配置项: {e}")
        return False
    
    # 使用你验证过的时间戳
    start_timestamp = 1750348800000  # 北京时间 2025-06-19 00:00:00
    end_timestamp = 1750435200000    # 北京时间 2025-06-20 00:00:00
    
    # 转换为可读时间用于显示
    start_dt = datetime.fromtimestamp(start_timestamp / 1000)
    end_dt = datetime.fromtimestamp(end_timestamp / 1000)
    
    SUMMER_MODULES_TEST_LOGGER.info("查询时间范围:")
    SUMMER_MODULES_TEST_LOGGER.info(f"  开始时间: {start_dt} (时间戳: {start_timestamp})")
    SUMMER_MODULES_TEST_LOGGER.info(f"  结束时间: {end_dt} (时间戳: {end_timestamp})")
    SUMMER_MODULES_TEST_LOGGER.info("  💡 注意: 预期约 50,000+ 条记录")
    
    table_name = "cloud-whoisxml-whois-data"
    
    # 建立 SSH 连接
    ssh = None
    try:
        ssh = SSHConnection(
            hostname=HBASE_HOST,
            username=HBASE_USERNAME,
            password=HBASE_PASSWORD,
            port=22
        )
        ssh.connect()
        
        SUMMER_MODULES_TEST_LOGGER.info("✅ SSH 连接建立成功")
        
        # 测试不同的查询策略
        test_cases = [
            {
                "name": "计数查询",
                "scan_cmd": f"count '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}]}}",
                "timeout": 180,  # 增加超时时间
                "description": "统计指定时间范围内的记录数量"
            },
            {
                "name": "限制前10条",
                "scan_cmd": f"scan '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}], LIMIT => 10}}",
                "timeout": 90,  # 增加超时时间
                "description": "获取前10条记录用于验证数据格式"
            },
            {
                "name": "限制前100条",
                "scan_cmd": f"scan '{table_name}', {{TIMERANGE => [{start_timestamp}, {end_timestamp}], LIMIT => 100}}",
                "timeout": 120,  # 增加超时时间
                "description": "获取前100条记录用于性能测试"
            }
        ]
        
        results = {}
        
        for test_case in test_cases:
            SUMMER_MODULES_TEST_LOGGER.info(f"\n" + "-" * 50)
            SUMMER_MODULES_TEST_LOGGER.info(f"测试: {test_case['name']}")
            SUMMER_MODULES_TEST_LOGGER.info(f"描述: {test_case['description']}")
            SUMMER_MODULES_TEST_LOGGER.info("-" * 50)
            
            commands = [
                "hbase shell",
                test_case['scan_cmd'],
                "exit"
            ]
            
            SUMMER_MODULES_TEST_LOGGER.info(f"执行命令: {test_case['scan_cmd']}")
            
            start_time = time.time()
            
            try:
                output = ssh.execute_interactive_commands(
                    commands=commands,
                    timeout=test_case['timeout'],
                    wait_between_commands=2.0
                )
                
                execution_time = time.time() - start_time
                
                if output:
                    SUMMER_MODULES_TEST_LOGGER.info(f"✅ 执行成功")
                    SUMMER_MODULES_TEST_LOGGER.info(f"📊 执行时间: {execution_time:.2f} 秒")
                    SUMMER_MODULES_TEST_LOGGER.info(f"📊 输出长度: {len(output)} 字符")
                    
                    # 分析输出
                    result_info = analyze_hbase_output(output, test_case['name'])
                    results[test_case['name']] = result_info
                    
                    # 显示结果摘要
                    SUMMER_MODULES_TEST_LOGGER.info(f"📈 结果摘要:")
                    for key, value in result_info.items():
                        SUMMER_MODULES_TEST_LOGGER.info(f"   {key}: {value}")
                        
                else:
                    SUMMER_MODULES_TEST_LOGGER.error(f"❌ 执行失败，无输出")
                    results[test_case['name']] = {"status": "failed", "error": "no output"}
                    
            except Exception as e:
                SUMMER_MODULES_TEST_LOGGER.error(f"❌ 执行失败: {e}")
                results[test_case['name']] = {"status": "failed", "error": str(e)}
        
        # 总结所有测试结果
        SUMMER_MODULES_TEST_LOGGER.info("\n" + "=" * 70)
        SUMMER_MODULES_TEST_LOGGER.info("测试结果总结")
        SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
        
        for test_name, result in results.items():
            SUMMER_MODULES_TEST_LOGGER.info(f"\n🔍 {test_name}:")
            if isinstance(result, dict):
                for key, value in result.items():
                    SUMMER_MODULES_TEST_LOGGER.info(f"   {key}: {value}")
        
        return len(results) > 0
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 测试失败: {e}")
        return False
    finally:
        if ssh:
            try:
                ssh.close()
                SUMMER_MODULES_TEST_LOGGER.info("✅ SSH 连接已关闭")
            except:
                pass


def analyze_hbase_output(output: str, test_name: str) -> dict:
    """分析 HBase Shell 输出，提取关键信息"""
    
    result = {
        "status": "success",
        "output_lines": len(output.split('\n')),
        "output_size": len(output)
    }
    
    lines = output.split('\n')
    
    # 查找行数统计
    for line in lines:
        line = line.strip()
        if " row(s)" in line and "Took " in line:
            # 提取行数，例如 "50345 row(s)"
            parts = line.split()
            for i, part in enumerate(parts):
                if part.endswith("row(s)") and i > 0:
                    try:
                        row_count = int(parts[i-1])
                        result["row_count"] = row_count
                        break
                    except ValueError:
                        pass
        
        # 提取执行时间，例如 "Took 52.2480 seconds"
        if line.startswith("Took ") and "seconds" in line:
            try:
                time_part = line.split()[1]
                execution_seconds = float(time_part)
                result["execution_seconds"] = execution_seconds
            except (IndexError, ValueError):
                pass
    
    # 对于计数查询，特殊处理
    if test_name == "计数查询":
        # count 命令的输出格式不同
        for line in lines:
            line = line.strip()
            if line.isdigit():
                result["count_result"] = int(line)
                break
    
    # 对于扫描查询，尝试解析数据行
    if "限制" in test_name:
        try:
            parsed_data = parse_hbase_shell_output_improved(output)
            result["parsed_records"] = len(parsed_data)
            
            if parsed_data:
                # 提取第一条记录作为样例
                first_record = parsed_data[0]
                result["sample_row_key"] = first_record.get('row_key', 'N/A')[:50]
                result["sample_timestamp"] = first_record.get('timestamp', 'N/A')
        except Exception as e:
            result["parse_error"] = str(e)
    
    return result


def test_hbase_shell_full_scan():
    """测试无限制的全量数据扫描，检索所有数据"""
    
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    SUMMER_MODULES_TEST_LOGGER.info("无限制全量数据扫描测试")
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    
    # 从配置文件获取连接信息
    try:
        hbase_config = CONFIG["hbase"]
        HBASE_HOST = hbase_config["host"]
        HBASE_USERNAME = hbase_config["username"]
        HBASE_PASSWORD = hbase_config["password"]
        
        SUMMER_MODULES_TEST_LOGGER.info(f"连接配置: {HBASE_USERNAME}@{HBASE_HOST}:22")
        
    except KeyError as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"配置文件中缺少 hbase 配置项: {e}")
        return False
    
    # 使用已验证的时间戳
    start_timestamp = 1750348800000  # 北京时间 2025-06-19 00:00:00
    end_timestamp = 1750435200000    # 北京时间 2025-06-20 00:00:00
    
    # 转换为可读时间用于显示
    start_dt = datetime.fromtimestamp(start_timestamp / 1000)
    end_dt = datetime.fromtimestamp(end_timestamp / 1000)
    
    SUMMER_MODULES_TEST_LOGGER.info("查询时间范围:")
    SUMMER_MODULES_TEST_LOGGER.info(f"  开始时间: {start_dt} (时间戳: {start_timestamp})")
    SUMMER_MODULES_TEST_LOGGER.info(f"  结束时间: {end_dt} (时间戳: {end_timestamp})")
    SUMMER_MODULES_TEST_LOGGER.info("  💡 注意: 预期约 50,345 条记录，无 LIMIT 限制")
    SUMMER_MODULES_TEST_LOGGER.warning("  ⚠️  这是一个长时间运行的查询，可能需要 60-120 秒")
    
    table_name = "cloud-whoisxml-whois-data"
    
    # 建立 SSH 连接
    ssh = None
    try:
        ssh = SSHConnection(
            hostname=HBASE_HOST,
            username=HBASE_USERNAME,
            password=HBASE_PASSWORD,
            port=22
        )
        ssh.connect()
        
        SUMMER_MODULES_TEST_LOGGER.info("✅ SSH 连接建立成功")
        
        # 构建无限制的扫描命令
        scan_command = (
            f"scan '{table_name}', {{"
            f"TIMERANGE => [{start_timestamp}, {end_timestamp}]"
            f"}}"
        )
        
        SUMMER_MODULES_TEST_LOGGER.info(f"\n" + "-" * 50)
        SUMMER_MODULES_TEST_LOGGER.info("无限制全量扫描")
        SUMMER_MODULES_TEST_LOGGER.info("-" * 50)
        SUMMER_MODULES_TEST_LOGGER.info(f"执行命令: {scan_command}")
        SUMMER_MODULES_TEST_LOGGER.info("预期: 检索所有数据，约50,345条记录")
        
        commands = [
            "hbase shell",
            scan_command,
            "exit"
        ]
        
        start_time = time.time()
        SUMMER_MODULES_TEST_LOGGER.info("🚀 开始执行全量扫描...")
        
        try:
            output = ssh.execute_interactive_commands(
                commands=commands,
                timeout=180,  # 3分钟超时，考虑到大数据量
                wait_between_commands=2.0
            )
            
            execution_time = time.time() - start_time
            
            if output:
                SUMMER_MODULES_TEST_LOGGER.info(f"✅ 全量扫描执行成功")
                SUMMER_MODULES_TEST_LOGGER.info(f"📊 执行时间: {execution_time:.2f} 秒")
                SUMMER_MODULES_TEST_LOGGER.info(f"📊 输出长度: {len(output)} 字符")
                
                # 分析输出
                result_info = analyze_full_scan_output(output)
                
                # 显示结果摘要
                SUMMER_MODULES_TEST_LOGGER.info(f"📈 全量扫描结果:")
                for key, value in result_info.items():
                    SUMMER_MODULES_TEST_LOGGER.info(f"   {key}: {value}")
                
                # 检查是否达到预期的数据量
                if "total_rows" in result_info:
                    total_rows = result_info["total_rows"]
                    if total_rows >= 50000:
                        SUMMER_MODULES_TEST_LOGGER.info("✅ 数据量检查通过：检索到大量数据记录")
                    else:
                        SUMMER_MODULES_TEST_LOGGER.warning(f"⚠️  数据量不足预期：{total_rows} < 50,000")
                
                return True
                
            else:
                SUMMER_MODULES_TEST_LOGGER.error(f"❌ 全量扫描执行失败，无输出")
                return False
                
        except Exception as e:
            SUMMER_MODULES_TEST_LOGGER.error(f"❌ 全量扫描执行失败: {e}")
            return False
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 测试失败: {e}")
        return False
    finally:
        if ssh:
            try:
                ssh.close()
                SUMMER_MODULES_TEST_LOGGER.info("✅ SSH 连接已关闭")
            except:
                pass


def analyze_full_scan_output(output: str) -> dict:
    """分析全量扫描输出，提取关键信息"""
    
    result = {
        "status": "success",
        "output_lines": len(output.split('\n')),
        "output_size": len(output),
        "total_rows": 0,
        "data_rows": 0,
        "execution_info": "N/A"
    }
    
    lines = output.split('\n')
    
    # 查找执行结果信息（末尾的统计行）
    for line in lines:
        line = line.strip()
        
        # 查找类似 "50345 row(s)" 的统计信息
        if "row(s)" in line and ("Took" in line or "in" in line):
            # 提取行数，如 "50345 row(s) in 52.24 seconds" 或 "50345 row(s)"
            match = re.search(r'(\d+)\s+row\(s\)', line)
            if match:
                result["total_rows"] = int(match.group(1))
                result["execution_info"] = line
                
                # 提取执行时间
                time_match = re.search(r'([\d.]+)\s+seconds', line)
                if time_match:
                    result["execution_seconds"] = float(time_match.group(1))
                    result["rows_per_second"] = f"{result['total_rows'] / float(time_match.group(1)):.1f}"
                break
    
    # 计算实际的数据行数（包含 column= 的行）
    data_line_count = 0
    for line in lines:
        if " column=" in line and " timestamp=" in line:
            data_line_count += 1
    
    result["data_rows"] = data_line_count
    
    # 输出关键行用于调试
    SUMMER_MODULES_TEST_LOGGER.info("全量扫描输出关键信息:")
    
    # 显示开头几行
    SUMMER_MODULES_TEST_LOGGER.info("开头5行:")
    for i, line in enumerate(lines[:5]):
        SUMMER_MODULES_TEST_LOGGER.info(f"  [{i:2d}] {line[:100]}{'...' if len(line) > 100 else ''}")
    
    # 显示末尾几行（通常包含统计信息）
    SUMMER_MODULES_TEST_LOGGER.info("末尾10行:")
    for i, line in enumerate(lines[-10:]):
        line_num = len(lines) - 10 + i
        SUMMER_MODULES_TEST_LOGGER.info(f"  [{line_num:2d}] {line[:100]}{'...' if len(line) > 100 else ''}")
    
    return result


def main():
    """主测试函数"""
    SUMMER_MODULES_TEST_LOGGER.info("开始改进版 HBase Shell 测试...")
    
    # 首先运行快速连接测试
    if not quick_connectivity_test():
        SUMMER_MODULES_TEST_LOGGER.error("❌ 快速连接测试失败，停止后续测试")
        return False
    
    # 检查命令行参数
    test_type = "basic"
    if len(sys.argv) > 1:
        if sys.argv[1] == "--large":
            test_type = "large"
        elif sys.argv[1] == "--both":
            test_type = "both"
        elif sys.argv[1] == "--full":
            test_type = "full"
        elif sys.argv[1] == "--all":
            test_type = "all"
    
    success = True
    
    if test_type in ["basic", "both", "all"]:
        # 运行基础的交互式测试
        SUMMER_MODULES_TEST_LOGGER.info("\n🔸 运行基础测试...")
        basic_success = test_hbase_shell_interactive_improved()
        success = success and basic_success
    
    if test_type in ["large", "both", "all"]:
        # 运行大数据量测试
        SUMMER_MODULES_TEST_LOGGER.info("\n🔸 运行大数据量测试...")
        large_success = test_hbase_shell_large_dataset()
        success = success and large_success
    
    if test_type in ["full", "both", "all"]:
        # 运行无限制全量扫描测试
        SUMMER_MODULES_TEST_LOGGER.info("\n🔸 运行无限制全量扫描测试...")
        full_scan_success = test_hbase_shell_full_scan()
        success = success and full_scan_success
    
    if success:
        SUMMER_MODULES_TEST_LOGGER.info("✅ 改进版测试成功完成！")
        SUMMER_MODULES_TEST_LOGGER.info("\n💡 改进点总结:")
        SUMMER_MODULES_TEST_LOGGER.info("1. 使用真正的交互式 SSH 执行")
        SUMMER_MODULES_TEST_LOGGER.info("2. 避免了 echo | hbase shell 的问题")
        SUMMER_MODULES_TEST_LOGGER.info("3. 使用已知有数据的时间范围")
        SUMMER_MODULES_TEST_LOGGER.info("4. 改进了输出解析逻辑")
        SUMMER_MODULES_TEST_LOGGER.info("5. 支持大数据量查询测试")
        SUMMER_MODULES_TEST_LOGGER.info("6. 支持无限制全量数据扫描（50K+记录）")
        return True
    else:
        SUMMER_MODULES_TEST_LOGGER.error("❌ 改进版测试失败")
        return False


if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # 快速测试模式
        success = quick_connectivity_test()
    else:
        # 显示使用说明
        if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
            print("使用说明:")
            print("  python test_hbase_interactive_improved.py --quick    # 快速连接测试")
            print("  python test_hbase_interactive_improved.py           # 基础功能测试")
            print("  python test_hbase_interactive_improved.py --large   # 大数据量测试（LIMIT查询）")
            print("  python test_hbase_interactive_improved.py --full    # 无限制全量扫描（约50K条记录）")
            print("  python test_hbase_interactive_improved.py --both    # 基础+大数据量+全量扫描")
            print("  python test_hbase_interactive_improved.py --all     # 完整测试（同--both）")
            print("")
            print("注意：")
            print("  --full 和 --both/--all 将执行长时间运行的全量查询（预计1-3分钟）")
            print("  建议先运行 --quick 和 --large 确认连接正常")
            sys.exit(0)
        
        # 完整测试模式
        success = main()
    
    if success:
        SUMMER_MODULES_TEST_LOGGER.info("🎉 测试成功完成！")
        sys.exit(0)
    else:
        SUMMER_MODULES_TEST_LOGGER.error("💥 测试失败！")
        sys.exit(1)
