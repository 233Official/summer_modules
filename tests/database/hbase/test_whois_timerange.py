#!/usr/bin/env python3
"""
云端 Whois 数据时间范围查询优化方案
"""

import time
from datetime import datetime, timedelta
from summer_modules.database.hbase.hbase_api import HBaseAPI


def test_whois_timerange_queries():
    """测试不同的时间范围查询方案"""
    
    # HBase 连接配置
    hbase = HBaseAPI(host="your-hbase-host", port=9090)
    table_name = "cloud-whoisxml-whois-data"
    
    print("=" * 60)
    print("云端 Whois 数据时间范围查询测试")
    print("=" * 60)
    
    # 方案1：SSH + HBase Shell (推荐)
    print("\n1. SSH + HBase Shell 方案 (最高效)")
    print("-" * 40)
    
    try:
        # 查询最近1小时的数据
        current_time = int(time.time() * 1000)
        start_time = current_time - (1 * 60 * 60 * 1000)  # 1小时前
        
        result_shell = hbase.get_data_with_timerange_via_shell(
            table_name=table_name,
            start_timestamp=start_time,
            end_timestamp=current_time,
            ssh_host="your-hbase-server.com",
            ssh_user="your-username",
            ssh_key_path="/path/to/your/private-key",  # 或者 None 使用密码认证
            limit=100  # 限制返回100条记录
        )
        
        print(f"✅ SSH + HBase Shell 查询成功，返回 {len(result_shell)} 条记录")
        
        # 显示前几条记录
        for i, row in enumerate(result_shell[:3]):
            print(f"  示例记录 {i+1}: {row['row_key']}")
            if 'cf' in row and 'type' in row['cf']:
                print(f"    类型: {row['cf']['type']['value']}")
                
    except Exception as e:
        print(f"❌ SSH + HBase Shell 查询失败: {e}")
    
    # 方案2：优化的最近数据查询
    print("\n2. 优化的最近数据查询 (中等效率)")
    print("-" * 40)
    
    try:
        result_optimized = hbase.get_recent_data_optimized(
            table_name=table_name,
            hours_back=1,  # 查询最近1小时
            limit=100
        )
        
        print(f"✅ 优化查询成功，返回 {len(result_optimized)} 条记录")
        
        # 分析反向时间戳
        for i, row in enumerate(result_optimized[:3]):
            row_key = row['row_key']
            if '-' in row_key:
                domain, reverse_ts = row_key.rsplit('-', 1)
                try:
                    reverse_timestamp = int(reverse_ts)
                    normal_timestamp = hbase.reverse_timestamp_to_normal(reverse_timestamp)
                    dt = datetime.fromtimestamp(normal_timestamp / 1000)
                    print(f"  记录 {i+1}: {domain} (时间: {dt})")
                except:
                    print(f"  记录 {i+1}: {row_key}")
                    
    except Exception as e:
        print(f"❌ 优化查询失败: {e}")
    
    # 方案3：特定域名模式查询 (如果知道域名)
    print("\n3. 特定域名模式查询 (最高效，但需要知道域名)")
    print("-" * 40)
    
    try:
        # 如果知道特定域名，可以进行精确查询
        domain_pattern = "example.com"  # 替换为实际域名
        
        result_domain = hbase.get_recent_data_optimized(
            table_name=table_name,
            hours_back=24,
            domain_pattern=domain_pattern,
            limit=50
        )
        
        print(f"✅ 域名模式查询成功，返回 {len(result_domain)} 条记录")
        
    except Exception as e:
        print(f"❌ 域名模式查询失败: {e}")
    
    # 反向时间戳计算示例
    print("\n4. 反向时间戳计算示例")
    print("-" * 40)
    
    # 显示反向时间戳的计算逻辑
    current_ts = int(time.time() * 1000)
    reverse_ts = hbase.calculate_reverse_timestamp(current_ts)
    recovered_ts = hbase.reverse_timestamp_to_normal(reverse_ts)
    
    print(f"当前时间戳: {current_ts}")
    print(f"反向时间戳: {reverse_ts}")
    print(f"恢复时间戳: {recovered_ts}")
    print(f"时间戳一致: {current_ts == recovered_ts}")
    
    # 时间范围示例
    one_hour_ago = current_ts - (60 * 60 * 1000)
    reverse_current = hbase.calculate_reverse_timestamp(current_ts)
    reverse_hour_ago = hbase.calculate_reverse_timestamp(one_hour_ago)
    
    print(f"\n时间范围计算:")
    print(f"1小时前时间戳: {one_hour_ago}")
    print(f"当前时间戳: {current_ts}")
    print(f"对应反向时间戳范围: {reverse_current} ~ {reverse_hour_ago}")
    print(f"(注意: 反向时间戳中，较小的值表示更新的时间)")
    
    hbase.close()
    print("\n测试完成!")


def create_ssh_config_example():
    """创建 SSH 配置示例"""
    ssh_config = """
# SSH 配置示例
# 文件路径: ~/.ssh/config

Host hbase-server
    HostName your-hbase-server.com
    User your-username
    Port 22
    IdentityFile ~/.ssh/your-private-key
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
"""
    
    with open("/tmp/ssh_config_example.txt", "w") as f:
        f.write(ssh_config)
    
    print("SSH 配置示例已保存到 /tmp/ssh_config_example.txt")


def benchmark_query_methods():
    """性能基准测试"""
    import time
    
    hbase = HBaseAPI(host="your-hbase-host", port=9090)
    table_name = "cloud-whoisxml-whois-data"
    
    current_time = int(time.time() * 1000)
    start_time = current_time - (1 * 60 * 60 * 1000)  # 1小时前
    
    print("性能基准测试")
    print("=" * 40)
    
    # 测试原始全表扫描方法
    print("1. 测试原始全表扫描方法...")
    start = time.time()
    try:
        result1 = hbase.get_data_with_timerange(
            table_name, start_time, current_time, include_timestamp=True
        )
        duration1 = time.time() - start
        print(f"   全表扫描: {len(result1)} 条记录, 耗时: {duration1:.2f}秒")
    except Exception as e:
        print(f"   全表扫描失败: {e}")
    
    # 测试优化方法
    print("2. 测试优化查询方法...")
    start = time.time()
    try:
        result2 = hbase.get_recent_data_optimized(
            table_name, hours_back=1, limit=1000
        )
        duration2 = time.time() - start
        print(f"   优化查询: {len(result2)} 条记录, 耗时: {duration2:.2f}秒")
    except Exception as e:
        print(f"   优化查询失败: {e}")
    
    # 如果可以使用 SSH 方法
    print("3. 测试 SSH + HBase Shell 方法...")
    print("   (需要配置 SSH 连接)")
    
    hbase.close()


if __name__ == "__main__":
    # 运行测试
    test_whois_timerange_queries()
    
    # 创建配置示例
    create_ssh_config_example()
    
    # 运行性能测试 (可选)
    # benchmark_query_methods()
