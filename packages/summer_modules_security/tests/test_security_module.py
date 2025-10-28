from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, Dict

import httpx

import pytest

import summer_modules_security._storage as storage
from summer_modules_security import set_storage_root


@pytest.fixture(autouse=True)
def security_storage_root(tmp_path, monkeypatch):
    set_storage_root(tmp_path)
    importlib.reload(storage)
    modules = [
        "summer_modules_security.vulnerability.cve",
        "summer_modules_security.vulnerability.cve.poc",
        "summer_modules_security.vulnerability.github_repo.nuclei",
        "summer_modules_security.vulnerability.cnnvd",
        "summer_modules_security.vulnerability.cnnvd.info",
        "summer_modules_security.threat_intelligence.otx.otx_api",
    ]
    for module_name in modules:
        importlib.reload(importlib.import_module(module_name))
    yield


def test_storage_dir_env_var(tmp_path):
    set_storage_root(tmp_path)
    path = storage.get_storage_dir("demo")
    assert path.parent == tmp_path
    storage.set_storage_root(tmp_path / "alt")
    path2 = storage.get_storage_dir("demo2")
    assert path2.parent == tmp_path / "alt"


def test_cve_info_local_cache(tmp_path):
    from summer_modules_security.vulnerability.cve.info import CVEInfo

    data_dir = tmp_path / "cve"
    cve_info = CVEInfo(translator=lambda text: f"{text}_cn", storage_dir=data_dir)

    class DummyResponse:
        status_code = 200

        def json(self):
            return {
                "containers": {
                    "cna": {
                        "descriptions": [
                            {"lang": "en", "value": "Example description"},
                            {"lang": "fr", "value": "Description FR"},
                        ]
                    }
                }
            }

    class DummyClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers):
            return DummyResponse()

    result = cve_info._get_vul_info_from_cve("CVE-2025-0001", DummyClient(), {})
    assert result["containers"]["cna"]["descriptions"]["cn"] == "Example description_cn"
    cached = cve_info.get_cve_description("CVE-2025-0001", lang="cn")
    assert cached == "Example description_cn"


def test_exp_link_fetch_and_cache(tmp_path):
    from summer_modules_security.vulnerability.cve.poc import (
        get_exp_link_list_from_exploit_db,
    )

    class DummyResponse:
        def __init__(self, payload, status_code=200):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    class DummyClient:
        def __init__(self):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            self.calls += 1
            if "exploit-db.com/search" in url:
                return DummyResponse(
                    {
                        "data": [
                            {"id": 1},
                            {"id": 2},
                        ]
                    }
                )
            raise AssertionError(f"Unexpected URL: {url}")

    result = get_exp_link_list_from_exploit_db(
        "CVE-2025-0001", enable_local_search=False, client_factory=DummyClient
    )
    assert result["success"]
    assert result["exp_link_list"] == [
        "https://www.exploit-db.com/exploits/1",
        "https://www.exploit-db.com/exploits/2",
    ]

    cached_result = get_exp_link_list_from_exploit_db(
        "CVE-2025-0001", enable_local_search=True, client_factory=lambda: None
    )
    assert cached_result["success"]
    assert cached_result["exp_link_list"] == result["exp_link_list"]


def test_get_exp_poc_link_list_uses_nuclei_cache(tmp_path):
    from summer_modules_security.vulnerability.cve.poc import get_exp_poc_link_list

    nuclei_cache = {"CVE-2025-0002": "https://example.com/poc"}
    exp_result = get_exp_poc_link_list(
        "CVE-2025-0002",
        enable_local_search=False,
        nuclei_cache=nuclei_cache,
        client_factory=lambda: DummyExploitClient(),
    )
    assert "https://example.com/poc" in exp_result["exp_poc_link_list"]


class DummyExploitClient:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, headers=None):
        class Response:
            status_code = 200

            def json(self_inner):
                return {"data": [{"id": 1234}]}

        return Response()


def test_get_write_nuclei_http_cve_dict(tmp_path):
    from summer_modules_security.vulnerability.github_repo.nuclei import (
        get_write_nuclei_http_cve_dict,
    )

    class DummyResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code
            self.text = json.dumps(payload)

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("error", request=None, response=None)

    class DummyClient:
        def __init__(self):
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            self.calls.append(url)
            if url.endswith("/contents/http/cves"):
                return DummyResponse([{"path": "/http/cves/2024"}])
            return DummyResponse(
                [{"name": "CVE-2024-0001.yaml", "path": "http/cves/2024/CVE-2024-0001.yaml"}]
            )

    cache_path = tmp_path / "nuclei_cache.json"
    result = get_write_nuclei_http_cve_dict(
        github_token="token",
        local_path=cache_path,
        enable_local_cache=False,
        client_factory=DummyClient,
    )
    assert result == {"CVE-2024-0001": "https://github.com/projectdiscovery/nuclei-templates/tree/main/http/cves/2024/CVE-2024-0001.yaml"}
    assert json.loads(cache_path.read_text(encoding="utf-8")) == result


def test_otx_api_saves_data(tmp_path):
    from summer_modules_security.threat_intelligence.otx.otx_api import (
        OTXApi,
        SEARCH_PULSE_URL,
    )

    class DummyClient:
        def __init__(self):
            self.calls: Dict[str, Dict[str, Any]] = {}

        def get(self, url, **kwargs):
            self.calls[url] = kwargs
            if url == SEARCH_PULSE_URL:
                return {
                    "results": [
                        {
                            "id": "pulse-1",
                            "subscriber_count": 10,
                        }
                    ]
                }
            if url.endswith("/pulses/pulse-1"):
                return {"id": "pulse-1"}
            return {"results": []}

    data_dir = tmp_path / "otx"
    api = OTXApi(
        "fake-key",
        data_dir=data_dir,
        http_client=DummyClient(),
        auto_save_interval=0,
        register_atexit=False,
    )

    response = api.otx_search_pulses()
    assert response["results"][0]["id"] == "pulse-1"
    api.save_data()
    assert (data_dir / "otx_pulses_base_info.json").exists()
    detail = api.get_pulses_info("pulse-1")
    assert detail["id"] == "pulse-1"
