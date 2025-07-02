#!/usr/bin/env python3
"""
HBase 时间范围查询实用示例
展示不同场景下的最佳实践
"""

import time
from datetime import datetime, timedelta
from summer_modules.database.hbase.hbase_api import HBaseAPI


def main():
    """主要示例函数"""
    
    # HBase 连接配置
    hbase = HBaseAPI(host="your-hbase-host", port=9090)
    table_name = "cloud-whoisxml-whois-data"
    
    print("=" * 70)
    print("HBase 时间范围查询最佳实践示例")
    print("=" * 70)
    
    # 计算时间范围
    current_time = int(time.time() * 1000)
    one_hour_ago = current_time - (1 * 60 * 60 * 1000)
    one_day_ago = current_time - (24 * 60 * 60 * 1000)
    
    print(f"当前时间: {datetime.fromtimestamp(current_time/1000)}")
    print(f"1小时前: {datetime.fromtimestamp(one_hour_ago/1000)}")
    print(f"1天前: {datetime.fromtimestamp(one_day_ago/1000)}")
    
    # =====================================
    # 场景1: 未知域名的时间范围查询 (推荐方案)
    # =====================================
    print("\n" + "="*50)
    print("场景1: 未知域名的时间范围查询")
    print("推荐方案: SSH + HBase Shell")
    print("="*50)
    
    try:
        result_shell = hbase.get_data_with_timerange_via_shell(
            table_name=table_name,
            start_timestamp=one_hour_ago,
            end_timestamp=current_time,
            ssh_host="your-hbase-server.com",
            ssh_user="your-username",
            ssh_password="your-password",  # 推荐从配置文件或环境变量读取
            ssh_port=22,
            limit=100,
            timeout=120
        )
        
        print(f"✅ SSH + HBase Shell 查询成功")
        print(f"📊 返回记录数: {len(result_shell)}")
        print(f"⚡ 性能: 最佳 (服务器端 TIMERANGE 过滤)")
        
        # 显示示例记录
        for i, row in enumerate(result_shell[:3]):
            print(f"  📝 记录 {i+1}: {row['row_key']}")
            
    except Exception as e:
        print(f"❌ SSH + HBase Shell 查询失败: {e}")
        print("💡 请检查SSH连接配置和权限")
    
    # =====================================
    # 场景2: 已知域名的时间范围查询 (高效方案)
    # =====================================
    print("\n" + "="*50)
    print("场景2: 已知域名的时间范围查询")
    print("推荐方案: 反向时间戳优化")
    print("="*50)
    
    known_domain = "example.com"  # 替换为实际域名
    
    try:
        result_domain = hbase.get_recent_data_optimized(
            table_name=table_name,
            hours_back=24,
            domain_pattern=known_domain,  # 关键：提供域名模式
            limit=50
        )
        
        print(f"✅ 域名模式查询成功")
        print(f"🎯 目标域名: {known_domain}")
        print(f"📊 返回记录数: {len(result_domain)}")
        print(f"⚡ 性能: 高 (利用行键排序优化)")
        
        # 分析返回的数据
        for i, row in enumerate(result_domain[:3]):
            row_key = row['row_key']
            if '-' in row_key:
                domain, reverse_ts = row_key.rsplit('-', 1)
                try:
                    reverse_timestamp = int(reverse_ts)
                    normal_timestamp = hbase.reverse_timestamp_to_normal(reverse_timestamp)
                    dt = datetime.fromtimestamp(normal_timestamp / 1000)
                    print(f"  📝 记录 {i+1}: {domain} (时间: {dt})")
                except:
                    print(f"  📝 记录 {i+1}: {row_key}")
                    
    except Exception as e:
        print(f"❌ 域名模式查询失败: {e}")
    
    # =====================================
    # 场景3: 错误示例 - 未知域名使用反向时间戳
    # =====================================
    print("\n" + "="*50)
    print("场景3: 错误示例 - 未知域名使用反向时间戳优化")
    print("⚠️  警告: 这会导致全表扫描，性能很差！")
    print("="*50)
    
    try:
        # 故意不提供 domain_pattern 来展示性能问题
        print("🚨 执行未优化查询（仅用于演示，请勿在生产环境使用）...")
        
        result_bad = hbase.get_recent_data_optimized(
            table_name=table_name,
            hours_back=1,  # 只查询1小时，减少影响
            # domain_pattern=None,  # 未提供域名 - 这是问题所在
            limit=10  # 限制很小的数量
        )
        
        print(f"⚠️  低效查询完成")
        print(f"📊 返回记录数: {len(result_bad)}")
        print(f"🐌 性能: 很差 (全表扫描+客户端过滤)")
        print("💡 生产环境请使用 SSH + HBase Shell 方案！")
        
    except Exception as e:
        print(f"❌ 查询失败: {e}")
    
    # =====================================
    # 工具函数示例
    # =====================================
    print("\n" + "="*50)
    print("反向时间戳工具函数示例")
    print("="*50)
    
    # 演示反向时间戳计算
    test_timestamp = current_time
    reverse_ts = hbase.calculate_reverse_timestamp(test_timestamp)
    recovered_ts = hbase.reverse_timestamp_to_normal(reverse_ts)
    
    print(f"原始时间戳: {test_timestamp}")
    print(f"反向时间戳: {reverse_ts}")
    print(f"恢复时间戳: {recovered_ts}")
    print(f"计算正确性: {'✅ 正确' if test_timestamp == recovered_ts else '❌ 错误'}")
    
    # 演示时间排序
    print("\n时间排序演示 (反向时间戳让新数据排前面):")
    timestamps = [current_time, one_hour_ago, one_day_ago]
    reverse_timestamps = [hbase.calculate_reverse_timestamp(ts) for ts in timestamps]
    
    for i, (normal_ts, reverse_ts) in enumerate(zip(timestamps, reverse_timestamps)):
        dt = datetime.fromtimestamp(normal_ts / 1000)
        print(f"  {i+1}. 时间: {dt}, 反向时间戳: {reverse_ts}")
    
    print(f"反向时间戳排序 (小到大): {sorted(reverse_timestamps)}")
    print("💡 注意: 反向时间戳越小，实际时间越新")
    
    # 关闭连接
    hbase.close()
    print("\n🔚 示例完成，连接已关闭")


def performance_comparison_demo():
    """性能对比演示（谨慎运行）"""
    print("\n" + "="*70)
    print("性能对比演示 (请在测试环境运行)")
    print("="*70)
    
    hbase = HBaseAPI(host="your-hbase-host", port=9090)
    table_name = "cloud-whoisxml-whois-data"
    
    current_time = int(time.time() * 1000)
    start_time = current_time - (1 * 60 * 60 * 1000)  # 1小时前
    
    methods = [
        {
            "name": "SSH + HBase Shell",
            "func": lambda: hbase.get_data_with_timerange_via_shell(
                table_name, start_time, current_time,
                "your-hbase-server.com", "username", "/path/to/key", limit=100
            ),
            "expected_performance": "最佳"
        },
        {
            "name": "反向时间戳+已知域名",
            "func": lambda: hbase.get_recent_data_optimized(
                table_name, hours_back=1, domain_pattern="example.com", limit=100
            ),
            "expected_performance": "良好"
        },
        {
            "name": "反向时间戳+未知域名",
            "func": lambda: hbase.get_recent_data_optimized(
                table_name, hours_back=1, limit=10  # 小限制避免性能问题
            ),
            "expected_performance": "差"
        }
    ]
    
    for method in methods:
        print(f"\n测试方法: {method['name']}")
        print(f"预期性能: {method['expected_performance']}")
        
        try:
            start_time_test = time.time()
            result = method['func']()
            end_time_test = time.time()
            
            duration = end_time_test - start_time_test
            print(f"✅ 执行时间: {duration:.2f} 秒")
            print(f"📊 返回记录数: {len(result)}")
            
        except Exception as e:
            print(f"❌ 执行失败: {e}")
    
    hbase.close()


if __name__ == "__main__":
    # 运行主要示例
    main()
    
    # 可选：运行性能对比（请在测试环境运行）
    # performance_comparison_demo()
