from pathlib import Path

from tests import SUMMER_MODULES_TEST_LOGGER, CONFIG
from tests.database.postgres import CURRENT_DIR


from summer_modules.database.postgres.data_export import PostgresExporter

POSTGRES_CONFIG = CONFIG["database"]["postgres"]


def test_export_to_jsonl():
    """
    测试 PostgresExporter 的 export_to_jsonl 方法
    """
    exporter = PostgresExporter(
        db_name=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["username"],
        password=POSTGRES_CONFIG["password"],
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
    )

    output_dir = CURRENT_DIR / "output"
    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)
    # 定义要导出的表名
    table_name = "whois_records"

    # 执行导出操作
    exporter.export_to_jsonl(table_name, output_dir)

def main():
    test_export_to_jsonl()


if __name__ == "__main__":
    main()