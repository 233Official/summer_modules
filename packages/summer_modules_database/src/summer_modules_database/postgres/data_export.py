"""
postgres 数据导出模块
"""

import json
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from . import POSTGRES_LOGGER


class PostgresExporter:
    def __init__(self, db_name, user, password, host, port=5432):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def export_to_jsonl(
        self, table_name: str, output_dir: Path, batch_size: int = 10000
    ) -> None:
        """
        将指定的 PostgreSQL 表导出为 JSONL 格式文件

        Args:
            table_name (str): 要导出的表名
            output_dir (Path): 输出目录，导出的 JSONL 文件将保存在此目录下
            batch_size (int): 每批次导出的记录数，默认为 10000
        """
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        try:
            # 连接数据库并使用字典行工厂以便直接得到可序列化的 dict
            with psycopg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.db_name,
            ) as conn:
                with conn.cursor(row_factory=dict_row) as cursor:
                    # 获取总记录数
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = cursor.fetchone()
                    total_records = result["count"] if result else 0
                    print(f"总记录数: {total_records}")

                    # 分批参数
                    total_batches = (total_records + batch_size - 1) // batch_size

                    for batch_num in range(1, total_batches + 1):
                        offset = (batch_num - 1) * batch_size

                        # 查询当前批次数据
                        cursor.execute(
                            f"""SELECT * FROM {table_name} 
                               ORDER BY id 
                               LIMIT %s OFFSET %s""",
                            (batch_size, offset),
                        )

                        # 准备输出文件
                        output_file = output_dir / f"{table_name}_batch{batch_num}.jsonl"

                        # 写入JSONL文件
                        with open(output_file, "w") as f:
                            for record in cursor:
                                # 处理日期时间类型，确保可以序列化为JSON
                                record_dict = dict(record)
                                for key, value in record_dict.items():
                                    if hasattr(value, "isoformat"):  # 对日期时间类型进行转换
                                        record_dict[key] = value.isoformat()

                                # 写入一行JSON
                                f.write(json.dumps(record_dict) + "\n")

                        print(f"已导出批次 {batch_num}/{total_batches} 到文件: {output_file}")

            print("数据导出完成")

        except Exception as e:
            print(f"导出失败: {e}")
