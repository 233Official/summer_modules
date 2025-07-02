#!/usr/bin/env python3
"""
测试 HBase API 时间范围查询功能
"""

import time
from datetime import datetime, timedelta

from summer_modules.database.hbase.hbase_api import HBaseAPI


def test_timerange_query():
    """测试时间范围查询功能"""
    
    # 连接参数 - 需要根据实际环境调整
    host = "localhost"
    port = 9090
    
    try:
        # 创建 HBase API 实例
        hbase = HBaseAPI(host=host, port=port)
        
        # 测试表名
        test_table = "test_timerange_table"
        
        # 创建测试表
        column_families = ["cf1", "cf2"]
        if not hbase.table_exists(test_table):
            print(f"创建测试表: {test_table}")
            hbase.create_table(test_table, column_families)
        
        # 获取当前时间戳
        current_time = int(time.time() * 1000)  # 毫秒
        
        # 准备测试数据，插入不同时间戳的数据
        test_data = []
        for i in range(5):
            # 每隔1小时插入一条数据
            timestamp_offset = i * 60 * 60 * 1000  # 1小时的毫秒数
            row_key = f"test_row_{i}"
            data = {
                "cf1": {
                    "col1": f"value_{i}",
                    "col2": {"nested": f"data_{i}"}
                },
                "cf2": {
                    "status": "active" if i % 2 == 0 else "inactive",
                    "timestamp": current_time + timestamp_offset
                }
            }
            test_data.append((row_key, data))
        
        # 插入测试数据
        print("插入测试数据...")
        hbase.put_rows(test_table, test_data)
        
        # 等待一下确保数据写入
        time.sleep(2)
        
        # 测试时间范围查询
        print("\n测试时间范围查询...")
        
        # 查询范围：从当前时间到未来2小时
        start_timestamp = current_time
        end_timestamp = current_time + (2 * 60 * 60 * 1000)  # 2小时后
        
        print(f"查询时间范围: {start_timestamp} - {end_timestamp}")
        print(f"查询时间范围: {datetime.fromtimestamp(start_timestamp/1000)} - {datetime.fromtimestamp(end_timestamp/1000)}")
        
        # 执行时间范围查询
        result = hbase.get_data_with_timerange(test_table, start_timestamp, end_timestamp)
        
        print(f"\n查询结果数量: {len(result)}")
        
        # 显示查询结果
        for i, row in enumerate(result):
            print(f"\n行 {i+1}:")
            print(f"  行键: {row['row_key']}")
            
            for cf_name, cf_data in row.items():
                if cf_name == "row_key":
                    continue
                    
                print(f"  列族 {cf_name}:")
                for qual_name, qual_data in cf_data.items():
                    if isinstance(qual_data, dict) and "timestamp" in qual_data:
                        timestamp = qual_data["timestamp"]
                        value = qual_data["value"]
                        dt = datetime.fromtimestamp(timestamp/1000)
                        print(f"    {qual_name}: {value} (时间戳: {timestamp}, 时间: {dt})")
                    else:
                        print(f"    {qual_name}: {qual_data}")
        
        # 测试空范围查询
        print("\n\n测试空时间范围查询...")
        past_start = current_time - (10 * 60 * 60 * 1000)  # 10小时前
        past_end = current_time - (5 * 60 * 60 * 1000)     # 5小时前
        
        empty_result = hbase.get_data_with_timerange(test_table, past_start, past_end)
        print(f"空范围查询结果数量: {len(empty_result)}")
        
        # 清理测试表
        print(f"\n清理测试表: {test_table}")
        hbase.delete_table(test_table)
        
        # 关闭连接
        hbase.close()
        
        print("\n时间范围查询测试完成!")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_timerange_query()
