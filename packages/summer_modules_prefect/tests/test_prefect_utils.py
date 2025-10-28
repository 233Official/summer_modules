from __future__ import annotations

from pathlib import Path
import subprocess
from types import SimpleNamespace
from typing import Sequence

import pytest
from prefect.exceptions import ObjectNotFound

from summer_modules_prefect import (
    check_flow_deployment,
    check_flow_deployment_sync,
    check_work_pool,
    check_work_pool_sync,
    export_poetry_requirements_to_pips,
)


class StubRunner:
    def __init__(self) -> None:
        self.called_with: list[tuple[list[str], Path | None]] = []

    def __call__(self, command: Sequence[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
        payload = list(command)
        self.called_with.append((payload, cwd))
        return subprocess.CompletedProcess(payload, 0, stdout="", stderr="")


@pytest.fixture()
def anyio_backend():
    return "asyncio"


class DummyClient:
    def __init__(self, *, work_pool_exists: bool = True, deployments: list[str] | None = None) -> None:
        self._work_pool_exists = work_pool_exists
        self._deployments = deployments or []

    async def __aenter__(self) -> "DummyClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def read_work_pool(self, work_pool_name: str) -> None:
        if not self._work_pool_exists:
            raise ObjectNotFound(http_exc=Exception(f"{work_pool_name} not found"))

    async def read_deployments(self):
        return [SimpleNamespace(name=name) for name in self._deployments]


def test_export_poetry_requirements_to_pips(tmp_path: Path) -> None:
    runner = StubRunner()
    output_path = export_poetry_requirements_to_pips(
        tmp_path,
        command_runner=runner,
    )

    assert output_path == tmp_path / "requirements.txt"
    assert runner.called_with == [
        (
            [
                "poetry",
                "export",
                "-f",
                "requirements.txt",
                "--output",
                str(tmp_path / "requirements.txt"),
                "--without-hashes",
            ],
            tmp_path,
        )
    ]


@pytest.mark.anyio("asyncio")
async def test_check_work_pool_creates_when_missing(tmp_path: Path) -> None:
    runner = StubRunner()
    created = await check_work_pool(
        "demo",
        tmp_path,
        command_runner=runner,
        client_factory=lambda: DummyClient(work_pool_exists=False),
    )

    assert created is True
    assert runner.called_with[0][0][:4] == ["prefect", "work-pool", "create", "demo"]
    assert runner.called_with[0][1] == tmp_path


@pytest.mark.anyio("asyncio")
async def test_check_work_pool_noop_when_exists(tmp_path: Path) -> None:
    runner = StubRunner()
    created = await check_work_pool(
        "exists",
        tmp_path,
        command_runner=runner,
        client_factory=lambda: DummyClient(work_pool_exists=True),
    )

    assert created is False
    assert runner.called_with == []


@pytest.mark.anyio("asyncio")
async def test_check_flow_deployment(tmp_path: Path) -> None:
    client = DummyClient(deployments=["alpha", "beta"])
    exists = await check_flow_deployment(
        "alpha",
        client_factory=lambda: client,
    )
    missing = await check_flow_deployment(
        "gamma",
        client_factory=lambda: client,
    )

    assert exists is True
    assert missing is False


def test_sync_wrappers(tmp_path: Path) -> None:
    runner = StubRunner()
    created = check_work_pool_sync(
        "demo",
        tmp_path,
        command_runner=runner,
        client_factory=lambda: DummyClient(work_pool_exists=False),
    )
    assert created is True

    exists = check_flow_deployment_sync(
        "alpha",
        client_factory=lambda: DummyClient(deployments=["alpha"]),
    )
    assert exists is True
