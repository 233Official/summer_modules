"""Example script for exporting PostgreSQL data."""

from pathlib import Path

from summer_modules_database.postgres.data_export import PostgresExporter

from . import CURRENT_DIR, get_postgres_config


def main() -> None:
    config = get_postgres_config()
    if not config:
        raise RuntimeError("请在 config.toml 的 [database.postgres] 中配置连接信息。")

    exporter = PostgresExporter(
        db_name=config["database"],
        user=config["username"],
        password=config["password"],
        host=config["host"],
        port=config.get("port", 5432),
    )

    output_dir = CURRENT_DIR / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    table_name = config.get("table", "whois_records")

    exporter.export_to_jsonl(table_name, output_dir)


if __name__ == "__main__":
    main()
