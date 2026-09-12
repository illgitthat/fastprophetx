"""Check required operations in the current public ProphetX Swagger."""

from __future__ import annotations

import httpx

from fastprophetx.endpoint_presence import (
    REQUIRED_ENDPOINTS,
    SWAGGER_URL,
    contract_violations,
)


def main() -> None:
    response = httpx.get(SWAGGER_URL, timeout=15.0, follow_redirects=False)
    response.raise_for_status()
    document = response.json()
    violations = sorted(contract_violations(document))
    if violations:
        raise SystemExit(f"Swagger contract violations: {violations}")
    print(
        f"Swagger contract check passed: {len(REQUIRED_ENDPOINTS)} required operations"
    )


if __name__ == "__main__":
    main()
