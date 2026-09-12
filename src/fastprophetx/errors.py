"""Structured ProphetX exceptions with secret-safe diagnostics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

_SENSITIVE_PARTS = ("authorization", "access_key", "secret", "token", "password")


def sanitize(value: Any) -> Any:
    """Return a recursively redacted diagnostic value."""

    if isinstance(value, Mapping):
        return {
            str(key): (
                "<redacted>"
                if any(part in str(key).lower() for part in _SENSITIVE_PARTS)
                else sanitize(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [sanitize(item) for item in value]
    if isinstance(value, str) and value.lower().startswith("bearer "):
        return "Bearer <redacted>"
    return value


class ProphetXAPIError(RuntimeError):
    """Base API exception."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        code: str | int | None = None,
        details: Any = None,
        raw_payload: Any = None,
        outcome_unknown: bool = False,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.details = sanitize(details)
        self.raw_payload = sanitize(raw_payload)
        self.outcome_unknown = outcome_unknown


class AuthenticationError(ProphetXAPIError):
    """Authentication failed or expired."""


class PreSubmitError(ProphetXAPIError):
    """An order was rejected locally before a write was attempted."""


class TransportError(ProphetXAPIError):
    """A network transport failed."""


class ResponseError(ProphetXAPIError):
    """A successful HTTP response violated the expected contract."""


ProphetXAuthenticationError = AuthenticationError
ProphetXPreSubmitError = PreSubmitError
ProphetXTransportError = TransportError
ProphetXResponseError = ResponseError
