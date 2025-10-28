from __future__ import annotations

import types

import paramiko
import pytest

from summer_modules_ssh import SSHConnection


class FakeChannel:
    def __init__(self, exit_status: int = 0):
        self._exit_status = exit_status

    def recv_exit_status(self) -> int:
        return self._exit_status


class FakeStream:
    def __init__(self, data: str, exit_status: int = 0):
        self._data = data.encode("utf-8")
        self.channel = FakeChannel(exit_status)

    def read(self) -> bytes:
        return self._data


class FakeShell:
    def __init__(self, responses: dict[str, str]):
        self._responses = responses
        self._buffer = b""
        self._index = 0
        self.closed = False

    def send(self, data: bytes) -> None:
        command = data.decode("utf-8").strip()
        response = self._responses.get(command, "")
        payload = f"{command}\n{response}\nuser@host:~$ "
        self._buffer = payload.encode("utf-8")
        self._index = 0

    def recv_ready(self) -> bool:
        return self._index < len(self._buffer)

    def recv(self, size: int) -> bytes:
        chunk = self._buffer[self._index : self._index + size]
        self._index += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeSSHClient:
    def __init__(self, shell: FakeShell, command_outputs: dict[str, str]):
        self.shell = shell
        self.command_outputs = command_outputs
        self.connected = False

    def set_missing_host_key_policy(self, policy) -> None:
        pass

    def connect(self, **kwargs) -> None:
        self.connected = True

    def exec_command(self, command: str, timeout: int = 30):
        if command == "fail":
            stdout = FakeStream("", exit_status=1)
            stderr = FakeStream("error", exit_status=1)
        else:
            stdout = FakeStream(self.command_outputs.get(command, "ok"))
            stderr = FakeStream("")
        return None, stdout, stderr

    def invoke_shell(self, **kwargs):
        return self.shell

    def close(self) -> None:
        self.connected = False


@pytest.fixture
def fake_paramiko(monkeypatch):
    shell = FakeShell(
        {
            "whoami": "tester",
            "pwd": "/home/tester",
            "hbase shell": "hbase(main):001:0>",
        }
    )
    client = FakeSSHClient(shell, {"echo success": "success"})

    monkeypatch.setattr(paramiko, "SSHClient", lambda: client)
    monkeypatch.setattr(paramiko, "AutoAddPolicy", lambda: object())
    return client, shell


def test_execute_command_success(fake_paramiko):
    client, _ = fake_paramiko
    ssh = SSHConnection(hostname="host", username="user", password="pwd")
    ssh.connect()

    result = ssh.execute_command("echo success")
    assert result.success
    assert result.output.strip() == "success"
    ssh.close()


def test_execute_command_failure(fake_paramiko):
    client, _ = fake_paramiko
    ssh = SSHConnection(hostname="host", username="user", password="pwd")
    ssh.connect()

    result = ssh.execute_command("fail")
    assert not result.success
    assert result.has_errors()
    ssh.close()


def test_execute_interactive_commands(fake_paramiko):
    client, shell = fake_paramiko
    ssh = SSHConnection(hostname="host", username="user", password="pwd")
    ssh.connect()

    result = ssh.execute_interactive_commands(["whoami", "pwd"], wait_between_commands=0)
    assert result is not None and result.success
    assert result.command_outputs["whoami"] == "tester"
    assert result.command_outputs["pwd"] == "/home/tester"

    ssh.close()
    assert shell.closed


