#!/usr/bin/env python3
"""
探测 HBase 表中数据的时间范围，用于确定有效的测试时间戳
"""

import time
from datetime import datetime, timedelta
from summer_modules.database.hbase.hbase_api import HBaseAPI
from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG


def probe_data_timerange():
    """探测表中实际存在数据的时间范围"""
    
    try:
        # 从配置文件获取连接信息
        hbase_config = CONFIG["hbase"]
        HBASE_HOST = hbase_config["host"]
        HBASE_THRIFT_PORT = hbase_config["port"]
        HBASE_USERNAME = hbase_config["username"]
        HBASE_PASSWORD = hbase_config["password"]
        
        ssh_config = {
            "ssh_host": HBASE_HOST,
            "ssh_user": HBASE_USERNAME,
            "ssh_password": HBASE_PASSWORD,
            "ssh_port": 22
        }
        
        hbase = HBaseAPI(
            host=HBASE_HOST, 
            port=HBASE_THRIFT_PORT,
            username=HBASE_USERNAME,
            password=HBASE_PASSWORD
        )
        table_name = "cloud-whoisxml-whois-data"
        
        SUMMER_MODULES_TEST_LOGGER.info("🔍 开始探测表中数据的时间范围...")
        
        # 尝试不同的时间范围
        time_ranges = [
            ("最近1天", 1),
            ("最近3天", 3),
            ("最近7天", 7),
            ("最近30天", 30),
            ("最近90天", 90),
            ("最近180天", 180),
            ("最近1年", 365)
        ]
        
        for name, days in time_ranges:
            current_time = int(time.time() * 1000)
            start_time = current_time - (days * 24 * 60 * 60 * 1000)
            
            start_dt = datetime.fromtimestamp(start_time / 1000)
            end_dt = datetime.fromtimestamp(current_time / 1000)
            
            SUMMER_MODULES_TEST_LOGGER.info(f"\n📅 测试{name}: {start_dt.strftime('%Y-%m-%d')} 到 {end_dt.strftime('%Y-%m-%d')}")
            
            try:
                result = hbase.get_data_with_timerange_via_shell(
                    table_name=table_name,
                    start_timestamp=start_time,
                    end_timestamp=current_time,
                    limit=5,  # 只取5条记录
                    timeout=120,
                    **ssh_config
                )
                
                if result:
                    SUMMER_MODULES_TEST_LOGGER.info(f"✅ {name}找到 {len(result)} 条数据")
                    
                    # 显示一条记录的详细信息
                    if result:
                        row = result[0]
                        SUMMER_MODULES_TEST_LOGGER.info(f"📝 示例记录:")
                        SUMMER_MODULES_TEST_LOGGER.info(f"   行键: {row.get('row_key', 'N/A')}")
                        
                        # 显示列族信息
                        for cf_name, cf_data in row.items():
                            if cf_name != 'row_key' and isinstance(cf_data, dict):
                                SUMMER_MODULES_TEST_LOGGER.info(f"   列族 {cf_name}: {len(cf_data)} 个字段")
                    
                    return True  # 找到数据就返回
                else:
                    SUMMER_MODULES_TEST_LOGGER.warning(f"❌ {name}未找到数据")
                    
            except Exception as e:
                SUMMER_MODULES_TEST_LOGGER.error(f"❌ {name}查询失败: {e}")
        
        # 如果所有时间范围都没找到数据，尝试检查表是否为空
        SUMMER_MODULES_TEST_LOGGER.warning("⚠️  所有时间范围都没找到数据，尝试简单的表扫描...")
        
        try:
            result = hbase.get_data_with_timerange_via_shell(
                table_name=table_name,
                start_timestamp=0,  # 从最早时间开始
                end_timestamp=int(time.time() * 1000),  # 到现在
                limit=1,  # 只取1条记录
                timeout=60,
                **ssh_config
            )
            
            if result:
                SUMMER_MODULES_TEST_LOGGER.info(f"✅ 表中确实有数据，找到 {len(result)} 条记录")
                row = result[0]
                SUMMER_MODULES_TEST_LOGGER.info(f"   行键: {row.get('row_key', 'N/A')}")
            else:
                SUMMER_MODULES_TEST_LOGGER.warning("❓ 表可能为空或所有数据都在未来时间戳")
                
        except Exception as e:
            SUMMER_MODULES_TEST_LOGGER.error(f"❌ 简单扫描也失败: {e}")
        
        hbase.close()
        return False
        
    except Exception as e:
        SUMMER_MODULES_TEST_LOGGER.error(f"❌ 探测过程失败: {e}")
        return False


if __name__ == "__main__":
    probe_data_timerange()
