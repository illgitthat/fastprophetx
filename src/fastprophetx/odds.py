"""American-odds and v4 depth helpers."""

from __future__ import annotations

import math
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from typing import Any

from .models import DepthLevel


def validate_american_odds(odds: int) -> int:
    if isinstance(odds, bool) or not isinstance(odds, int):
        raise TypeError("American odds must be an integer")
    if -99 <= odds <= 99:
        raise ValueError("American odds must be <= -100 or >= 100")
    return odds


def implied_probability(odds: int) -> float:
    validate_american_odds(odds)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    absolute = float(abs(odds))
    return absolute / (absolute + 100.0)


def normalized_cost(odds: int) -> float:
    """Return the normalized unit cost implied by one price."""

    return implied_probability(odds)


def stake_to_profit(stake: float, odds: int) -> float:
    validate_american_odds(odds)
    if not math.isfinite(stake) or stake < 0:
        raise ValueError("stake must be finite and non-negative")
    return stake * odds / 100.0 if odds > 0 else stake * 100.0 / abs(odds)


def profit_to_stake(profit: float, odds: int) -> float:
    validate_american_odds(odds)
    if not math.isfinite(profit) or profit < 0:
        raise ValueError("profit must be finite and non-negative")
    return profit * 100.0 / odds if odds > 0 else profit * abs(odds) / 100.0


def normalize_selection_levels(
    levels: Iterable[dict[str, Any]],
    *,
    strict: bool = False,
) -> tuple[DepthLevel, ...]:
    """Normalize v4 levels while keeping quantity authoritative and raw data."""

    normalized: list[DepthLevel] = []
    for level in levels:
        if not isinstance(level, dict):
            if strict:
                raise TypeError("selection level must be a dictionary")
            continue
        price = level.get("price")
        quantity = level.get("quantity")
        if (
            isinstance(price, bool)
            or not isinstance(price, int | float)
            or isinstance(quantity, bool)
            or not isinstance(quantity, int | float)
        ):
            if strict:
                raise ValueError("selection level requires numeric price and quantity")
            continue
        if not math.isfinite(float(price)) or not float(price).is_integer():
            if strict:
                raise ValueError("selection price must be a finite integer")
            continue
        int_price = int(price)
        try:
            validate_american_odds(int_price)
        except TypeError, ValueError:
            if strict:
                raise
            continue
        float_quantity = float(quantity)
        if not math.isfinite(float_quantity) or float_quantity <= 0:
            continue
        raw_value = level.get("value")
        value = (
            float(raw_value)
            if isinstance(raw_value, int | float) and not isinstance(raw_value, bool)
            else None
        )
        inconsistent = False
        if value is not None:
            try:
                inconsistent = Decimal(str(quantity)) != Decimal(str(raw_value))
            except InvalidOperation:
                inconsistent = True
        if strict and inconsistent:
            raise ValueError("selection quantity and value are inconsistent")
        normalized.append(
            DepthLevel(
                price=int_price,
                quantity=float_quantity,
                value=value,
                inconsistent=inconsistent,
                raw=dict(level),
            )
        )
    return tuple(normalized)
