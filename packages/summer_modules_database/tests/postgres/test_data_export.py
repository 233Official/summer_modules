import json
from pathlib import Path

import psycopg

from summer_modules_database.postgres.data_export import PostgresExporter


class DummyCursor:
    def __init__(self):
        self.dataset = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return {"count": len(self.dataset)}

    def __iter__(self):
        return iter(self.dataset)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.closed = True
        return False


class DummyConnection:
    def __init__(self):
        self.cursor_instance = DummyCursor()
        self.closed = False

    def cursor(self, cursor_factory=None):
        return self.cursor_instance

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def test_export_to_jsonl(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(psycopg, "connect", lambda **_: DummyConnection())

    exporter = PostgresExporter(
        db_name="db",
        user="user",
        password="pwd",
        host="localhost",
    )

    exporter.export_to_jsonl("test_table", tmp_path, batch_size=10)

    output_files = list(tmp_path.glob("*.jsonl"))
    assert output_files
    content = output_files[0].read_text().strip().splitlines()
    assert len(content) == 2
    first_record = json.loads(content[0])
    assert first_record == {"id": 1, "name": "Alice"}
