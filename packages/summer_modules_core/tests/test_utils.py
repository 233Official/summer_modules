import asyncio
import json
from datetime import datetime
from pathlib import Path

import pytest
from zoneinfo import ZoneInfo

from summer_modules_core import utils


def test_write_and_read_json_file(tmp_path: Path) -> None:
    data = {"key": "value", "number": 42}
    json_path = tmp_path / "sample.json"

    utils.write_dict_to_json_file(data, json_path, one_line=False)

    assert json.loads(json_path.read_text()) == data
    assert utils.read_json_file_to_dict(json_path) == data


def test_get_all_json_files(tmp_path: Path) -> None:
    json_dir = tmp_path / "configs"
    json_dir.mkdir()
    (json_dir / "a.json").write_text("{}")
    (json_dir / "b.json").write_text("{}")
    (json_dir / "notes.txt").write_text("ignore me")

    files = utils.get_all_json_files(json_dir)
    names = sorted(file.name for file in files)

    assert names == ["a.json", "b.json"]


def test_retry_eventually_succeeds() -> None:
    call_state = {"count": 0}

    @utils.retry(max_retries=3, delay=0)
    def flaky_operation() -> str:
        call_state["count"] += 1
        if call_state["count"] < 2:
            raise ValueError("Boom")
        return "ok"

    assert flaky_operation() == "ok"
    assert call_state["count"] == 2


def test_retry_raises_after_max_attempts() -> None:
    @utils.retry(max_retries=2, delay=0, on_permanent_failure=None)
    def always_fail() -> None:
        raise RuntimeError("still failing")

    with pytest.raises(RuntimeError):
        always_fail()


def test_async_retry_handles_failures() -> None:
    call_state = {"count": 0}

    @utils.retry(max_retries=3, delay=0)
    async def flaky_async_operation() -> str:
        call_state["count"] += 1
        if call_state["count"] < 2:
            raise ValueError("Boom")
        return "ok"

    result = asyncio.run(flaky_async_operation())
    assert result == "ok"
    assert call_state["count"] == 2


def test_convert_timestamp_to_timezone_time() -> None:
    timestamp = int(datetime(2025, 1, 1, tzinfo=ZoneInfo("UTC")).timestamp() * 1000)
    converted = utils.convert_timestamp_to_timezone_time(
        timestamp, ZoneInfo("Asia/Shanghai")
    )
    expected = datetime(2025, 1, 1, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    assert converted == expected


def test_convert_timezone_time_to_timezone_time() -> None:
    source_time = datetime(2025, 1, 1, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    converted = utils.convert_timezone_time_to_timezone_time(
        source_time, to_zone=ZoneInfo("UTC")
    )
    expected = datetime(2025, 1, 1, 4, 0, tzinfo=ZoneInfo("UTC"))

    assert converted == expected


def test_convert_timezone_time_to_utc() -> None:
    local_time = datetime(2025, 1, 1, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    converted = utils.convert_timezone_time_to_utc(local_time)
    expected = datetime(2025, 1, 1, 4, 0, tzinfo=ZoneInfo("UTC"))

    assert converted == expected
