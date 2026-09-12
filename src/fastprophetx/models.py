"""Small public value objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class OrderIntent:
    external_id: str
    strike_id: str
    price: int
    quantity: float
    order_strategy: str | None = None


@dataclass(frozen=True, slots=True)
class OrderResult:
    external_id: str
    outcome: str
    status: str
    payload: dict[str, Any] | None = None
    order_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class DepthLevel:
    price: int
    quantity: float
    value: float | None
    inconsistent: bool
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    event_id: int
    market_id: str
    strike_id: str
    outcome: str
    levels: tuple[DepthLevel, ...]
    received_at_monotonic: float
    received_at_wall: float
    exchange_timestamp: int | float | None
    sequence: int | None
    subtype: str | None = None


ProphetXOrderIntent = OrderIntent
ProphetXOrderResult = OrderResult
