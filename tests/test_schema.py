from __future__ import annotations

import httpx
import pytest

from fastprophetx.endpoint_presence import (
    SWAGGER_URL,
    contract_violations,
    missing_required_endpoints,
)


@pytest.mark.schema
def test_current_swagger_has_required_endpoint_presence() -> None:
    document = httpx.get(SWAGGER_URL, timeout=15.0).json()
    assert missing_required_endpoints(document) == set()
    assert contract_violations(document) == set()
