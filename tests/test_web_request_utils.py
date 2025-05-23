from summer_modules.web_request_utils import get_standard_domain_from_origin_domain
from tests.test_main import SUMMER_MODULES_TEST_LOGGER

standard_domain = get_standard_domain_from_origin_domain(
    "https://www.example.com:8080/path"
)

SUMMER_MODULES_TEST_LOGGER.debug(f"Standard domain: {standard_domain}")
