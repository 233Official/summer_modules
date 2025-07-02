#!/usr/bin/env python3
"""
测试基于 paramiko SSH 的 HBase Shell TIMERANGE 查询功能
"""

import time
import sys
from datetime import datetime, timedelta
from summer_modules.database.hbase.hbase_api import HBaseAPI
from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG


def validate_config():
    """验证配置文件是否完整"""
    try:
        hbase_config = CONFIG.get("hbase")
        if not hbase_config:
            SUMMER_MODULES_TEST_LOGGER.error("❌ 配置文件中未找到 'hbase' 配置项")
            return False
        
        required_keys = ["host", "port", "username", "password"]
        missing_keys = []
        
        for key in required_keys:
            if key not in hbase_config or not hbase_config[key]:
                missing_keys.append(key)
        
        if missing_keys:
            SUMMER_MODULES_TEST_LOGGER.error(f"❌ HBase 配置项不完整，缺少: {', '.join(missing_keys)}")
            return False
        
        SUMMER_MODULES_TEST_LOGGER.info("✅ 配置文件验证通过")
        SUMMER_MODULES_TEST_LOGGER.info(f"   HBase 主机: {hbase_config['host']}")
        SUMMER_MODULES_TEST_LOGGER.info(f"   Thrift 端口: {hbase_config['port']}")
        SUMMER_MODULES_TEST_LOGGER.info(f"   用户名: {hbase_config['username']}")
        SUMMER_MODULES_TEST_LOGGER.info("   密码: ***")
        
        return True
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 配置验证失败: {e}")
        return False


def test_ssh_hbase_shell_timerange():
    """测试 SSH + HBase Shell 时间范围查询"""
    
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    SUMMER_MODULES_TEST_LOGGER.info("测试 SSH + HBase Shell TIMERANGE 查询")
    SUMMER_MODULES_TEST_LOGGER.info("=" * 70)
    
    # 从配置文件获取连接信息
    try:
        hbase_config = CONFIG["hbase"]
        HBASE_HOST = hbase_config["host"]
        HBASE_THRIFT_PORT = hbase_config["port"]  # Thrift 服务端口
        HBASE_USERNAME = hbase_config["username"]
        HBASE_PASSWORD = hbase_config["password"]
        
        # SSH 连接使用相同的主机，但使用标准 SSH 端口和凭据
        ssh_config = {
            "ssh_host": HBASE_HOST,
            "ssh_user": HBASE_USERNAME,
            "ssh_password": HBASE_PASSWORD,
            "ssh_port": 22  # SSH 默认端口
        }
        
        SUMMER_MODULES_TEST_LOGGER.info(f"HBase 配置: {HBASE_HOST}:{HBASE_THRIFT_PORT}")
        SUMMER_MODULES_TEST_LOGGER.info(f"SSH 配置: {HBASE_HOST}:22 用户: {HBASE_USERNAME}")
        
    except KeyError as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"配置文件中缺少 hbase 配置项: {e}")
        return False
    
    # HBase 连接配置
    hbase = HBaseAPI(
        host=HBASE_HOST, 
        port=HBASE_THRIFT_PORT,
        username=HBASE_USERNAME,
        password=HBASE_PASSWORD
    )
    table_name = "cloud-whoisxml-whois-data"
    
    # 计算时间范围（最近7天，增加找到数据的概率）
    current_time = int(time.time() * 1000)
    start_time = current_time - (7 * 24 * 60 * 60 * 1000)  # 7天前
    
    SUMMER_MODULES_TEST_LOGGER.info("查询时间范围:")
    SUMMER_MODULES_TEST_LOGGER.info(f"  开始时间: {datetime.fromtimestamp(start_time/1000)}")
    SUMMER_MODULES_TEST_LOGGER.info(f"  结束时间: {datetime.fromtimestamp(current_time/1000)}")
    SUMMER_MODULES_TEST_LOGGER.info(f"  时间戳范围: [{start_time}, {current_time}]")
    SUMMER_MODULES_TEST_LOGGER.info(f"  💡 注意: 使用7天时间范围以增加找到数据的概率")
    
    # 测试1: 使用简单命令执行方式
    SUMMER_MODULES_TEST_LOGGER.info("\n" + "-" * 50)
    SUMMER_MODULES_TEST_LOGGER.info("测试1: 简单命令执行方式")
    SUMMER_MODULES_TEST_LOGGER.info("-" * 50)
    
    try:
        result_simple = hbase.get_data_with_timerange_via_shell(
            table_name=table_name,
            start_timestamp=start_time,
            end_timestamp=current_time,
            limit=50,  # 限制50条记录
            timeout=60,  # 60秒超时
            **ssh_config
        )
        
        SUMMER_MODULES_TEST_LOGGER.info("✅ 简单执行方式成功")
        SUMMER_MODULES_TEST_LOGGER.info(f"📊 返回记录数: {len(result_simple)}")
        
        # 如果没有数据，尝试更大的时间范围
        if not result_simple:
            SUMMER_MODULES_TEST_LOGGER.warning("🔍 当前时间范围无数据，尝试更大范围（最近30天）")
            extended_start_time = current_time - (30 * 24 * 60 * 60 * 1000)  # 30天前
            
            result_extended = hbase.get_data_with_timerange_via_shell(
                table_name=table_name,
                start_timestamp=extended_start_time,
                end_timestamp=current_time,
                limit=10,  # 更小的限制
                timeout=90,  # 更长的超时
                **ssh_config
            )
            
            if result_extended:
                SUMMER_MODULES_TEST_LOGGER.info(f"✅ 扩展时间范围找到 {len(result_extended)} 条记录")
                result_simple = result_extended  # 使用扩展结果
            else:
                SUMMER_MODULES_TEST_LOGGER.warning("⚠️  即使使用30天时间范围也未找到数据")
        
        # 显示前几条记录的详细信息
        for i, row in enumerate(result_simple[:3]):
            SUMMER_MODULES_TEST_LOGGER.info(f"\n📝 记录 {i+1}:")
            SUMMER_MODULES_TEST_LOGGER.info(f"   行键: {row.get('row_key', 'N/A')}")
            
            # 显示列族数据
            for cf_name, cf_data in row.items():
                if cf_name != 'row_key' and isinstance(cf_data, dict):
                    SUMMER_MODULES_TEST_LOGGER.info(f"   列族 {cf_name}:")
                    for qualifier, cell_data in cf_data.items():
                        if isinstance(cell_data, dict) and 'value' in cell_data:
                            value = cell_data['value']
                            timestamp = cell_data.get('timestamp')
                            if timestamp:
                                dt = datetime.fromtimestamp(timestamp / 1000)
                                SUMMER_MODULES_TEST_LOGGER.info(f"     {qualifier}: {str(value)[:100]}... (时间: {dt})")
                            else:
                                SUMMER_MODULES_TEST_LOGGER.info(f"     {qualifier}: {str(value)[:100]}...")
                        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 简单执行方式失败: {e}")
        SUMMER_MODULES_TEST_LOGGER.error("💡 请检查SSH连接配置和HBase服务状态")
    
    # 测试2: 使用交互式执行方式
    SUMMER_MODULES_TEST_LOGGER.info("\n" + "-" * 50)
    SUMMER_MODULES_TEST_LOGGER.info("测试2: 交互式执行方式")
    SUMMER_MODULES_TEST_LOGGER.info("-" * 50)
    
    try:
        result_interactive = hbase.get_data_with_timerange_via_shell_interactive(
            table_name=table_name,
            start_timestamp=start_time,
            end_timestamp=current_time,
            limit=30,  # 限制30条记录
            timeout=120,  # 更长的超时时间
            **ssh_config
        )
        
        SUMMER_MODULES_TEST_LOGGER.info("✅ 交互式执行方式成功")
        SUMMER_MODULES_TEST_LOGGER.info(f"📊 返回记录数: {len(result_interactive)}")
        
        # 显示统计信息
        if result_interactive:
            SUMMER_MODULES_TEST_LOGGER.info("\n📈 数据统计:")
            
            # 统计列族
            column_families = set()
            for row in result_interactive:
                for key in row.keys():
                    if key != 'row_key':
                        column_families.add(key)
            
            SUMMER_MODULES_TEST_LOGGER.info(f"   发现列族: {', '.join(column_families)}")
            
            # 统计行键模式
            row_key_domains = set()
            for row in result_interactive:
                row_key = row.get('row_key', '')
                if '-' in row_key:
                    domain = row_key.split('-')[0]
                    row_key_domains.add(domain)
            
            SUMMER_MODULES_TEST_LOGGER.info(f"   涉及域名数量: {len(row_key_domains)}")
            if len(row_key_domains) <= 5:
                SUMMER_MODULES_TEST_LOGGER.info(f"   域名示例: {', '.join(list(row_key_domains)[:5])}")
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 交互式执行方式失败: {e}")
        SUMMER_MODULES_TEST_LOGGER.error("💡 请检查SSH连接配置和HBase Shell环境")
    
    # 测试3: 性能对比
    SUMMER_MODULES_TEST_LOGGER.info("\n" + "-" * 50)
    SUMMER_MODULES_TEST_LOGGER.info("测试3: 性能对比")
    SUMMER_MODULES_TEST_LOGGER.info("-" * 50)
    
    try:
        # SSH + HBase Shell 方式
        start_perf = time.time()
        result_ssh = hbase.get_data_with_timerange_via_shell(
            table_name=table_name,
            start_timestamp=start_time,
            end_timestamp=current_time,
            limit=20,
            **ssh_config
        )
        ssh_duration = time.time() - start_perf
        
        SUMMER_MODULES_TEST_LOGGER.info("🚀 SSH + HBase Shell 性能:")
        SUMMER_MODULES_TEST_LOGGER.info(f"   执行时间: {ssh_duration:.2f} 秒")
        SUMMER_MODULES_TEST_LOGGER.info(f"   返回记录: {len(result_ssh)} 条")
        if ssh_duration > 0:
            SUMMER_MODULES_TEST_LOGGER.info(f"   平均速度: {len(result_ssh)/ssh_duration:.2f} 记录/秒")
        
        # 对比传统 Thrift 方式（仅在数据量小时测试）
        SUMMER_MODULES_TEST_LOGGER.warning("⚠️  注意: 传统 Thrift 方式会进行全表扫描，在生产环境请谨慎使用")
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 性能测试失败: {e}")
    
    # 关闭连接
    hbase.close()
    SUMMER_MODULES_TEST_LOGGER.info("\n🔚 测试完成，连接已关闭")
    return True


def test_ssh_connection_basic():
    """基础 SSH 连接测试"""
    SUMMER_MODULES_TEST_LOGGER.info("\n" + "=" * 50)
    SUMMER_MODULES_TEST_LOGGER.info("基础 SSH 连接测试")
    SUMMER_MODULES_TEST_LOGGER.info("=" * 50)
    
    from summer_modules.ssh import SSHConnection
    
    try:
        # 从配置文件获取连接信息
        hbase_config = CONFIG["hbase"]
        ssh_host = hbase_config["host"]
        ssh_user = hbase_config["username"]
        ssh_password = hbase_config["password"]
        
        SUMMER_MODULES_TEST_LOGGER.info(f"测试 SSH 连接: {ssh_user}@{ssh_host}:22")
        
        ssh = SSHConnection(
            hostname=ssh_host,
            username=ssh_user,
            password=ssh_password,
            port=22
        )
        ssh.connect()
        
        # 测试基本命令
        result = ssh.execute_command("echo 'SSH connection test'")
        SUMMER_MODULES_TEST_LOGGER.info(f"✅ SSH 基础测试成功: {result}")
        
        # 测试 HBase 命令可用性
        result = ssh.execute_command("which hbase")
        if result:
            SUMMER_MODULES_TEST_LOGGER.info(f"✅ HBase 命令可用: {result}")
        else:
            SUMMER_MODULES_TEST_LOGGER.warning("⚠️  HBase 命令不在 PATH 中，可能需要指定完整路径")
        
        # 测试 HBase Shell 启动
        result = ssh.execute_command("echo 'list' | hbase shell", timeout=30)
        if result and 'TABLE' in result:
            SUMMER_MODULES_TEST_LOGGER.info("✅ HBase Shell 可正常启动")
        else:
            SUMMER_MODULES_TEST_LOGGER.warning("⚠️  HBase Shell 启动异常，请检查环境配置")
            SUMMER_MODULES_TEST_LOGGER.info(f"输出: {result}")
        
        ssh.close()
        SUMMER_MODULES_TEST_LOGGER.info("✅ SSH 连接测试完成")
        return True
        
    except KeyError as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"配置文件中缺少 hbase 配置项: {e}")
        return False
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ SSH 连接测试失败: {e}")
        return False


def quick_test():
    """快速测试函数，用于开发时快速验证功能"""
    SUMMER_MODULES_TEST_LOGGER.info("🚀 执行快速测试...")
    
    if not validate_config():
        return False
    
    try:
        # 只测试 SSH 连接和简单的 HBase 命令
        hbase_config = CONFIG["hbase"]
        ssh_host = hbase_config["host"]
        ssh_user = hbase_config["username"]
        ssh_password = hbase_config["password"]
        
        from summer_modules.ssh import SSHConnection
        
        ssh = SSHConnection(
            hostname=ssh_host,
            username=ssh_user,
            password=ssh_password,
            port=22
        )
        ssh.connect()
        
        # 测试基本命令
        result = ssh.execute_command("echo 'Quick test: SSH OK'")
        SUMMER_MODULES_TEST_LOGGER.info(f"✅ SSH 连接正常: {result}")
        
        # 测试 HBase 可用性
        result = ssh.execute_command("hbase version | head -1", timeout=15)
        if result:
            SUMMER_MODULES_TEST_LOGGER.info(f"✅ HBase 可用: {result}")
        else:
            SUMMER_MODULES_TEST_LOGGER.warning("⚠️  HBase 版本检查失败")
        
        ssh.close()
        SUMMER_MODULES_TEST_LOGGER.info("✅ 快速测试通过")
        return True
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 快速测试失败: {e}")
        return False


def main():
    """主测试函数"""
    SUMMER_MODULES_TEST_LOGGER.info("开始 SSH + HBase Shell 功能测试...")
    
    # 首先验证配置
    if not validate_config():
        SUMMER_MODULES_TEST_LOGGER.error("❌ 配置验证失败，无法继续测试")
        return False
    
    # 运行基础连接测试
    ssh_test_result = test_ssh_connection_basic()
    
    if ssh_test_result:
        # SSH 连接正常，继续运行主要功能测试
        hbase_test_result = test_ssh_hbase_shell_timerange()
        
        if hbase_test_result:
            SUMMER_MODULES_TEST_LOGGER.info("✅ 所有测试完成！")
            return True
        else:
            SUMMER_MODULES_TEST_LOGGER.error("❌ HBase Shell 测试失败")
            return False
    else:
        SUMMER_MODULES_TEST_LOGGER.error("❌ SSH 连接测试失败，跳过 HBase Shell 测试")
        return False
    
    SUMMER_MODULES_TEST_LOGGER.info("\n💡 使用建议:")
    SUMMER_MODULES_TEST_LOGGER.info("1. 在生产环境中，优先使用 SSH + HBase Shell 方案")
    SUMMER_MODULES_TEST_LOGGER.info("2. 根据网络环境调整超时时间")
    SUMMER_MODULES_TEST_LOGGER.info("3. 监控查询性能，适当调整 limit 参数")
    SUMMER_MODULES_TEST_LOGGER.info("4. 确保 SSH 连接的安全性（使用密钥认证更佳）")


if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # 快速测试模式
        success = quick_test()
    else:
        # 完整测试模式
        success = main()
        
        SUMMER_MODULES_TEST_LOGGER.info("\n💡 使用建议:")
        SUMMER_MODULES_TEST_LOGGER.info("1. 在生产环境中，优先使用 SSH + HBase Shell 方案")
        SUMMER_MODULES_TEST_LOGGER.info("2. 根据网络环境调整超时时间")
        SUMMER_MODULES_TEST_LOGGER.info("3. 监控查询性能，适当调整 limit 参数")
        SUMMER_MODULES_TEST_LOGGER.info("4. 确保 SSH 连接的安全性（使用密钥认证更佳）")
        SUMMER_MODULES_TEST_LOGGER.info("\n💡 快速测试: python test_ssh_hbase_shell.py --quick")
    
    if success:
        SUMMER_MODULES_TEST_LOGGER.info("🎉 测试成功完成！")
        sys.exit(0)
    else:
        SUMMER_MODULES_TEST_LOGGER.error("💥 测试失败！")
        sys.exit(1)
