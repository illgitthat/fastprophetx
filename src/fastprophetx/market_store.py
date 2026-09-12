"""Authoritative ProphetX market structure with live depth overlay."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable, Sequence
from typing import Any

from .cache import EventDepthSnapshot, MarketCache, MarketKey


class MarketStore:
    """Own REST structure, reconciliation ordering, and executable live depth."""

    def __init__(
        self,
        client: Any,
        *,
        cache: MarketCache | None = None,
        reconcile_seconds: float = 300.0,
        disconnected_fallback_seconds: float = 30.0,
        monotonic: Any = time.monotonic,
    ) -> None:
        self.client = client
        self.cache = cache or MarketCache(monotonic=monotonic)
        self.reconcile_seconds = reconcile_seconds
        self.disconnected_fallback_seconds = disconnected_fallback_seconds
        self._monotonic = monotonic
        self._lock = threading.RLock()
        self._refresh_locks: dict[tuple[int, ...], threading.Lock] = {}
        self._structures: dict[int, list[dict[str, Any]]] = {}
        self._blueprints: dict[int, list[dict[str, Any]]] = {}
        self._reconciled_at: dict[int, float] = {}

    def markets(
        self,
        event_ids: Iterable[int],
        *,
        websocket_ready: bool = True,
    ) -> dict[int, list[dict[str, Any]]]:
        ids = _event_ids(event_ids)
        if not ids:
            return {}
        max_age = (
            self.reconcile_seconds
            if websocket_ready
            else self.disconnected_fallback_seconds
        )
        with self._lock:
            current = self._current_if_fresh_locked(ids, max_age)
            if current is not None:
                return current
        return self.refresh(
            ids,
            force=False,
            websocket_ready=websocket_ready,
        )

    def refresh(
        self,
        event_ids: Iterable[int],
        *,
        force: bool = True,
        websocket_ready: bool = True,
    ) -> dict[int, list[dict[str, Any]]]:
        ids = _event_ids(event_ids)
        if not ids:
            return {}
        lock_key = tuple(sorted(ids))
        with self._lock:
            refresh_lock = self._refresh_locks.setdefault(
                lock_key,
                threading.Lock(),
            )
        with refresh_lock:
            if not force:
                max_age = (
                    self.reconcile_seconds
                    if websocket_ready
                    else self.disconnected_fallback_seconds
                )
                with self._lock:
                    current = self._current_if_fresh_locked(ids, max_age)
                    if current is not None:
                        return current
            request = self.cache.begin_reconcile(ids)
            fetched: dict[str, list[dict[str, Any]]] = {}
            for start in range(0, len(ids), 50):
                fetched.update(
                    self.client.get_multiple_markets(ids[start : start + 50])
                )
            reconciled_at = self._monotonic()
            with self._lock:
                for event_id in ids:
                    event_key = str(event_id)
                    explicit = event_key in fetched
                    markets = fetched.get(event_key, [])
                    applied = self.cache.apply_authoritative_snapshot_result(
                        event_id,
                        {"markets": markets},
                        request=request,
                        received_at_monotonic=reconciled_at,
                    )
                    if not applied.accepted:
                        continue
                    previous_blueprint = self._blueprints.get(event_id, [])
                    if explicit:
                        blueprint = sanitize_market_blueprint(markets)
                        self._blueprints[event_id] = blueprint
                    else:
                        blueprint = previous_blueprint
                    state = self.cache.event_snapshot(event_id)
                    structure = blueprint
                    if not markets:
                        structure = retain_surviving_structure(state, blueprint)
                    self._structures[event_id] = structure
                    self._reconciled_at[event_id] = reconciled_at
                states = {
                    event_id: self.cache.event_snapshot(event_id) for event_id in ids
                }
                return self._materialize_locked(ids, states)

    def _current_if_fresh_locked(
        self,
        event_ids: Sequence[int],
        max_age: float,
    ) -> dict[int, list[dict[str, Any]]] | None:
        now = self._monotonic()
        states = {
            event_id: self.cache.event_snapshot(event_id) for event_id in event_ids
        }
        if not all(
            event_id in self._structures
            and states[event_id].valid
            and now - self._reconciled_at.get(event_id, 0.0) < max_age
            for event_id in event_ids
        ):
            return None
        return self._materialize_locked(event_ids, states)

    def clear_event(self, event_id: int) -> None:
        with self._lock:
            self._structures.pop(event_id, None)
            self._blueprints.pop(event_id, None)
            self._reconciled_at.pop(event_id, None)
            self.cache.clear_event(event_id)

    def invalidate_events(self, event_ids: Iterable[int]) -> None:
        self.cache.invalidate_events(event_ids)

    def _materialize_locked(
        self,
        event_ids: Sequence[int],
        states: dict[int, EventDepthSnapshot],
    ) -> dict[int, list[dict[str, Any]]]:
        result: dict[int, list[dict[str, Any]]] = {}
        for event_id in event_ids:
            state = states[event_id]
            if not state.valid:
                result[event_id] = []
                continue
            structure = self._structures.get(event_id, [])
            if not structure:
                structure = retain_surviving_structure(
                    state,
                    self._blueprints.get(event_id, []),
                )
            result[event_id] = materialize_markets(state, structure)
        return result


def _event_ids(values: Iterable[int]) -> tuple[int, ...]:
    ids = tuple(dict.fromkeys(values))
    if any(
        isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0
        for event_id in ids
    ):
        raise ValueError("event_ids must contain positive integers")
    return ids


def sanitize_market_blueprint(
    markets: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [_sanitize_market(market) for market in markets]


def _sanitize_market(market: dict[str, Any]) -> dict[str, Any]:
    result = dict(market)
    selections = market.get("selections")
    if isinstance(selections, list):
        result["selections"] = [
            [
                {
                    **selection,
                    "price": None,
                    "quantity": 0,
                    **({"value": 0} if "value" in selection else {}),
                }
                for selection in levels
                if isinstance(selection, dict)
            ]
            if isinstance(levels, list)
            else []
            for levels in selections
        ]
    strikes = market.get("market_strikes")
    if isinstance(strikes, list):
        result["market_strikes"] = [
            _sanitize_market(strike) for strike in strikes if isinstance(strike, dict)
        ]
    return result


def retain_surviving_structure(
    state: EventDepthSnapshot,
    markets: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not state.valid or not state.known_keys:
        return []
    retained: list[dict[str, Any]] = []
    for market in markets:
        filtered = _filter_market(
            state.event_id,
            market,
            state.known_keys,
        )
        if filtered is not None:
            retained.append(filtered)
    return retained


def _filter_market(
    event_id: int,
    market: dict[str, Any],
    surviving_keys: frozenset[MarketKey],
    *,
    parent_market_id: str | int | None = None,
) -> dict[str, Any] | None:
    result = dict(market)
    market_id = market.get("market_id")
    if market_id is None:
        market_id = (
            parent_market_id if parent_market_id is not None else market.get("id")
        )
    strike_id = market.get("strike_id")
    keep = False
    selections = market.get("selections")
    if isinstance(selections, list):
        filtered_groups: list[list[dict[str, Any]]] = []
        for levels in selections:
            representatives: dict[MarketKey, dict[str, Any]] = {}
            if isinstance(levels, list):
                for level in levels:
                    if not isinstance(level, dict):
                        continue
                    key = _selection_key(
                        event_id,
                        market_id,
                        strike_id,
                        level,
                    )
                    if key in surviving_keys and key not in representatives:
                        representatives[key] = dict(level)
            if representatives:
                filtered_groups.append(list(representatives.values()))
        result["selections"] = filtered_groups
        keep = bool(filtered_groups)
    strikes = market.get("market_strikes")
    if isinstance(strikes, list):
        filtered_strikes: list[dict[str, Any]] = []
        for child in strikes:
            if not isinstance(child, dict):
                continue
            filtered = _filter_market(
                event_id,
                child,
                surviving_keys,
                parent_market_id=market_id,
            )
            if filtered is not None:
                filtered_strikes.append(filtered)
        result["market_strikes"] = filtered_strikes
        keep = keep or bool(filtered_strikes)
    return result if keep else None


def materialize_markets(
    state: EventDepthSnapshot,
    markets: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not state.valid:
        return []
    return [_materialize_market(state, market) for market in markets]


def _materialize_market(
    state: EventDepthSnapshot,
    market: dict[str, Any],
    *,
    parent_market_id: str | int | None = None,
) -> dict[str, Any]:
    result = dict(market)
    market_id = market.get("market_id")
    if market_id is None:
        market_id = (
            parent_market_id if parent_market_id is not None else market.get("id")
        )
    strike_id = market.get("strike_id")
    selections = market.get("selections")
    if isinstance(selections, list):
        result["selections"] = [
            _materialize_group(state, market_id, strike_id, levels)
            if isinstance(levels, list)
            else []
            for levels in selections
        ]
    strikes = market.get("market_strikes")
    if isinstance(strikes, list):
        result["market_strikes"] = [
            _materialize_market(
                state,
                child,
                parent_market_id=market_id,
            )
            for child in strikes
            if isinstance(child, dict)
        ]
    return result


def _materialize_group(
    state: EventDepthSnapshot,
    market_id: str | int | None,
    strike_id: str | int | None,
    levels: list[Any],
) -> list[dict[str, Any]]:
    representatives: dict[MarketKey, dict[str, Any]] = {}
    for level in levels:
        if not isinstance(level, dict):
            continue
        key = _selection_key(
            state.event_id,
            market_id,
            strike_id,
            level,
        )
        if key is not None and key not in representatives:
            representatives[key] = level
    materialized: list[dict[str, Any]] = []
    for key, base in representatives.items():
        snapshot = state.snapshots.get(key)
        if key in state.known_keys and snapshot is not None:
            materialized.extend({**base, **level.raw} for level in snapshot.levels)
    return materialized


def _selection_key(
    event_id: int,
    market_id: str | int | None,
    strike_id: str | int | None,
    selection: dict[str, Any],
) -> MarketKey | None:
    selected_market_id = selection.get("market_id", market_id)
    selected_strike_id = selection.get("strike_id", strike_id)
    outcome = selection.get("outcome_id")
    if outcome is None:
        outcome = selection.get("name", selection.get("display_name"))
    if selected_market_id is None or selected_strike_id is None or outcome is None:
        return None
    return (
        event_id,
        str(selected_market_id),
        str(selected_strike_id),
        str(outcome),
    )
