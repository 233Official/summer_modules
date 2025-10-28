import pytest

from summer_modules_core.web_request_utils import (
    get_standard_domain_from_origin_domain,
)


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        ("example.com", "example.com"),
        (" www.example.com  ", "example.com"),
        ("https://www.example.com:8080/path", "example.com"),
        ("http://test.com", "test.com"),
        ("", ""),
    ],
)
def test_get_standard_domain_from_origin_domain(origin: str, expected: str) -> None:
    assert get_standard_domain_from_origin_domain(origin) == expected
