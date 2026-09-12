"""Thread-safe, sequence-aware in-memory ProphetX market depth."""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .models import DepthLevel, MarketSnapshot
from .odds import normalize_selection_levels, validate_american_odds

MarketKey = tuple[int, str, str, str]
CacheCallback = Callable[[MarketSnapshot], None]


@dataclass(frozen=True, slots=True)
class ReconcileRequest:
    revision: int
    event_ids: frozenset[int]
    started_at_monotonic: float
    invalidation_epochs: dict[int, int]


@dataclass(frozen=True, slots=True)
class AuthoritativeApplyResult:
    accepted: bool
    changed: bool


@dataclass(frozen=True, slots=True)
class EventDepthSnapshot:
    event_id: int
    valid: bool
    known_keys: frozenset[MarketKey]
    snapshots: dict[MarketKey, MarketSnapshot]


@dataclass(frozen=True, slots=True)
class _PreparedGroup:
    key: MarketKey
    replace_levels: bool
    normalized_levels: tuple[DepthLevel, ...]
    deletion_prices: tuple[int, ...]
    seen_prices: frozenset[int]


class MarketCache:
    """Lock-efficient market cache updated directly from WebSocket payloads."""

    def __init__(
        self,
        *,
        strict_depth: bool = False,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self.strict_depth = strict_depth
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._snapshots: dict[MarketKey, MarketSnapshot] = {}
        self._known_keys: set[MarketKey] = set()
        self._observed_at_by_key: dict[MarketKey, float] = {}
        self._level_changes: dict[
            tuple[MarketKey, int],
            tuple[float, DepthLevel | None],
        ] = {}
        self._selection_resets: dict[MarketKey, float] = {}
        self._membership_removals: dict[MarketKey, float] = {}
        self._event_replace_at: dict[int, float] = {}
        self._selection_keys_by_market: dict[
            tuple[int, str],
            tuple[MarketKey | None, ...],
        ] = {}
        self._selection_layout_observed_at: dict[tuple[int, str], float] = {}
        self._last_update_at_by_event: dict[int, float] = {}
        self._last_sequence: dict[tuple[int, str | None], int] = {}
        self._reconcile_revision = 0
        self._reconcile_revision_by_event: dict[int, int] = {}
        self._invalid_event_ids: set[int] = set()
        self._invalidation_epoch_by_event: dict[int, int] = {}
        self._callbacks: list[CacheCallback] = []
        self._seeded_event_ids: set[int] = set()
        self._seeding_event_ids: set[int] = set()
        self._connected = False
        self._ready = False
        self._authorized_scopes: frozenset[tuple[int, str | None]] = frozenset()
        self._rejected: tuple[dict[str, Any], ...] = ()
        self._last_error: str | None = None

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready

    @property
    def authorized_scopes(self) -> frozenset[tuple[int, str | None]]:
        with self._lock:
            return self._authorized_scopes

    @property
    def rejected_subscriptions(self) -> tuple[dict[str, Any], ...]:
        with self._lock:
            return self._rejected

    @property
    def last_error(self) -> str | None:
        with self._lock:
            return self._last_error

    def set_connection_state(
        self,
        *,
        connected: bool,
        ready: bool,
        authorized_scopes: Iterable[tuple[int, str | None]] = (),
        rejected: Iterable[dict[str, Any]] = (),
        error: str | None = None,
    ) -> None:
        with self._condition:
            self._connected = connected
            self._ready = ready
            self._authorized_scopes = frozenset(authorized_scopes)
            self._rejected = tuple(dict(item) for item in rejected)
            self._last_error = error
            self._condition.notify_all()

    def add_callback(self, callback: CacheCallback) -> None:
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def remove_callback(self, callback: CacheCallback) -> None:
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)

    def apply_update(
        self,
        event_id: int,
        payload: dict[str, Any],
        *,
        subtype: str | None = None,
        sequence: int | None = None,
        exchange_timestamp: int | float | None = None,
        received_at_monotonic: float | None = None,
        received_at_wall: float | None = None,
        replace: bool = False,
    ) -> bool:
        if event_id <= 0:
            raise ValueError("event_id must be positive")
        if not isinstance(payload, dict):
            raise TypeError("market update payload must be a dictionary")
        if sequence is None:
            sequence = _extract_sequence(payload)
        scope = (event_id, subtype)
        received_mono = (
            self._monotonic()
            if received_at_monotonic is None
            else received_at_monotonic
        )
        received_wall = (
            self._wall_clock() if received_at_wall is None else received_at_wall
        )
        with self._lock:
            selection_layout = dict(self._selection_keys_by_market)
        groups = _extract_groups(
            payload,
            event_id,
            force_replace=replace,
            selection_layout=selection_layout,
        )
        layout_updates = _extract_selection_layout(
            payload,
            event_id,
            selection_layout,
        )
        prepared_groups = tuple(
            _prepare_group(
                key,
                raw_levels,
                replace_levels,
                strict=self.strict_depth,
            )
            for key, raw_levels, replace_levels in groups
        )
        changed: list[MarketSnapshot] = []
        changed_any = False
        with self._condition:
            previous_sequence = self._last_sequence.get(scope)
            if (
                sequence is not None
                and previous_sequence is not None
                and sequence <= previous_sequence
            ):
                return False
            if sequence is not None:
                self._last_sequence[scope] = sequence
            if replace:
                self._event_replace_at[event_id] = received_mono
                changed_any = True
                for layout_key in tuple(self._selection_keys_by_market):
                    if layout_key[0] == event_id:
                        del self._selection_keys_by_market[layout_key]
                        self._selection_layout_observed_at.pop(layout_key, None)
                present_keys = {key for key, _, _ in groups}
                stale_keys = {
                    key
                    for key in self._known_keys
                    if key[0] == event_id and key not in present_keys
                }
                for key in stale_keys:
                    self._snapshots.pop(key, None)
                    self._known_keys.discard(key)
                    self._observed_at_by_key.pop(key, None)
                    self._membership_removals[key] = received_mono
                    for change_key in tuple(self._level_changes):
                        if change_key[0] == key:
                            del self._level_changes[change_key]
                    changed_any = True
            for prepared in prepared_groups:
                key = prepared.key
                replace_levels = prepared.replace_levels
                was_known = key in self._known_keys
                self._known_keys.add(key)
                self._observed_at_by_key[key] = received_mono
                if replace_levels:
                    self._selection_resets[key] = received_mono
                    for change_key in tuple(self._level_changes):
                        if change_key[0] == key:
                            del self._level_changes[change_key]
                old = self._snapshots.get(key)
                depth = (
                    {}
                    if replace_levels
                    else {level.price: level for level in old.levels}
                    if old
                    else {}
                )
                for deletion_price in prepared.deletion_prices:
                    depth.pop(deletion_price, None)
                    self._level_changes[(key, deletion_price)] = (
                        received_mono,
                        None,
                    )
                for level in prepared.normalized_levels:
                    depth[level.price] = level
                    self._level_changes[(key, level.price)] = (
                        received_mono,
                        level,
                    )
                if replace_levels:
                    depth = {
                        price: level
                        for price, level in depth.items()
                        if price in prepared.seen_prices
                    }
                if not depth:
                    if key in self._snapshots:
                        del self._snapshots[key]
                        changed_any = True
                    elif not was_known:
                        changed_any = True
                    continue
                levels = tuple(
                    sorted(depth.values(), key=lambda level: level.price, reverse=True)
                )
                snapshot = MarketSnapshot(
                    event_id=key[0],
                    market_id=key[1],
                    strike_id=key[2],
                    outcome=key[3],
                    levels=levels,
                    received_at_monotonic=received_mono,
                    received_at_wall=received_wall,
                    exchange_timestamp=(
                        exchange_timestamp
                        if exchange_timestamp is not None
                        else old.exchange_timestamp
                        if old
                        else None
                    ),
                    sequence=(
                        sequence
                        if sequence is not None
                        else old.sequence
                        if old
                        else None
                    ),
                    subtype=subtype,
                )
                self._snapshots[key] = snapshot
                changed.append(snapshot)
                changed_any = True
            self._selection_keys_by_market.update(layout_updates)
            for layout_key in layout_updates:
                self._selection_layout_observed_at[layout_key] = received_mono
            if changed_any:
                self._last_update_at_by_event[event_id] = received_mono
                self._condition.notify_all()
            callbacks = tuple(self._callbacks)
        for snapshot in changed:
            for callback in callbacks:
                callback(snapshot)
        return changed_any

    def begin_reconcile(self, event_ids: Iterable[int]) -> ReconcileRequest:
        ids = frozenset(event_ids)
        if not ids or any(
            isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0
            for event_id in ids
        ):
            raise ValueError("event_ids must contain positive integers")
        with self._condition:
            self._reconcile_revision += 1
            revision = self._reconcile_revision
            for event_id in ids:
                self._reconcile_revision_by_event[event_id] = revision
            return ReconcileRequest(
                revision=revision,
                event_ids=ids,
                started_at_monotonic=self._monotonic(),
                invalidation_epochs={
                    event_id: self._invalidation_epoch_by_event.get(event_id, 0)
                    for event_id in ids
                },
            )

    def apply_authoritative_snapshot(
        self,
        event_id: int,
        payload: dict[str, Any],
        *,
        request: ReconcileRequest,
        received_at_monotonic: float | None = None,
        received_at_wall: float | None = None,
    ) -> bool:
        return self.apply_authoritative_snapshot_result(
            event_id,
            payload,
            request=request,
            received_at_monotonic=received_at_monotonic,
            received_at_wall=received_at_wall,
        ).changed

    def apply_authoritative_snapshot_result(
        self,
        event_id: int,
        payload: dict[str, Any],
        *,
        request: ReconcileRequest,
        received_at_monotonic: float | None = None,
        received_at_wall: float | None = None,
    ) -> AuthoritativeApplyResult:
        """Apply REST as a base, then replay exact post-request WS changes."""

        if event_id not in request.event_ids:
            raise ValueError("event_id is not part of this reconciliation request")
        if not isinstance(payload, dict):
            raise TypeError("market snapshot payload must be a dictionary")
        received_mono = (
            self._monotonic()
            if received_at_monotonic is None
            else received_at_monotonic
        )
        received_wall = (
            self._wall_clock() if received_at_wall is None else received_at_wall
        )
        with self._lock:
            selection_layout = dict(self._selection_keys_by_market)
        groups = _extract_groups(
            payload,
            event_id,
            force_replace=True,
            selection_layout=selection_layout,
        )
        layout_updates = _extract_selection_layout(
            payload,
            event_id,
            selection_layout,
        )
        rest_depth: dict[MarketKey, dict[int, DepthLevel]] = {}
        for key, raw_levels, _replace_levels in groups:
            rest_depth[key] = {
                level.price: level
                for level in normalize_selection_levels(
                    raw_levels,
                    strict=self.strict_depth,
                )
            }

        changed: list[MarketSnapshot] = []
        changed_any = False
        with self._condition:
            if (
                self._reconcile_revision_by_event.get(event_id) != request.revision
                or self._invalidation_epoch_by_event.get(event_id, 0)
                != request.invalidation_epochs[event_id]
            ):
                return AuthoritativeApplyResult(accepted=False, changed=False)
            event_replace_at = self._event_replace_at.get(event_id)
            newer_event_replace = (
                event_replace_at is not None
                and event_replace_at > request.started_at_monotonic
            )
            newer_layouts = {
                layout_key: layout
                for layout_key, layout in self._selection_keys_by_market.items()
                if layout_key[0] == event_id
                and self._selection_layout_observed_at.get(layout_key, 0.0)
                > request.started_at_monotonic
            }
            for layout_key in tuple(self._selection_keys_by_market):
                if layout_key[0] == event_id:
                    del self._selection_keys_by_market[layout_key]
                    self._selection_layout_observed_at.pop(layout_key, None)
            if not newer_event_replace:
                self._selection_keys_by_market.update(layout_updates)
                for layout_key in layout_updates:
                    self._selection_layout_observed_at[layout_key] = received_mono
            self._selection_keys_by_market.update(newer_layouts)
            for layout_key in newer_layouts:
                self._selection_layout_observed_at[layout_key] = max(
                    received_mono,
                    self._selection_layout_observed_at.get(layout_key, 0.0),
                )
            if event_id in self._invalid_event_ids:
                self._invalid_event_ids.discard(event_id)
                changed_any = True
            newer_reset_keys = {
                key
                for key, reset_at in self._selection_resets.items()
                if key[0] == event_id and reset_at > request.started_at_monotonic
            }
            newer_membership_removals = {
                key: removed_at
                for key, removed_at in self._membership_removals.items()
                if key[0] == event_id and removed_at > request.started_at_monotonic
            }
            newer_changes_by_key: dict[
                MarketKey,
                list[tuple[float, int, DepthLevel | None]],
            ] = defaultdict(list)
            for (key, price), (changed_at, level) in self._level_changes.items():
                if key[0] == event_id and changed_at > request.started_at_monotonic:
                    newer_changes_by_key[key].append((changed_at, price, level))
            newer_change_keys = set(newer_changes_by_key)
            keys = (
                {key for key in self._known_keys if key[0] == event_id}
                | (set() if newer_event_replace else set(rest_depth))
                | newer_reset_keys
                | set(newer_membership_removals)
                | newer_change_keys
            )
            for key in keys:
                old = self._snapshots.get(key)
                depth = {} if newer_event_replace else dict(rest_depth.get(key, {}))
                reset_at = self._selection_resets.get(key)
                removed_at = newer_membership_removals.get(key)
                membership_removed_at = (
                    max(
                        timestamp
                        for timestamp in (event_replace_at, removed_at)
                        if timestamp is not None
                    )
                    if newer_event_replace or removed_at is not None
                    else None
                )
                if newer_event_replace or (
                    reset_at is not None and reset_at > request.started_at_monotonic
                ):
                    depth.clear()
                latest_change_at = received_mono
                for changed_at, price, level in sorted(
                    newer_changes_by_key.get(key, ()),
                    key=lambda item: item[0],
                ):
                    if (reset_at is not None and changed_at < reset_at) or (
                        removed_at is not None and changed_at < removed_at
                    ):
                        continue
                    latest_change_at = max(latest_change_at, changed_at)
                    if level is None:
                        depth.pop(price, None)
                    else:
                        depth[price] = level

                reset_restores = key in newer_reset_keys and (
                    membership_removed_at is None
                    or (reset_at is not None and reset_at >= membership_removed_at)
                )
                change_restores = any(
                    membership_removed_at is None or changed_at >= membership_removed_at
                    for changed_at, _price, _level in newer_changes_by_key.get(
                        key,
                        (),
                    )
                )
                keep_key = (
                    (
                        not newer_event_replace
                        and removed_at is None
                        and key in rest_depth
                    )
                    or reset_restores
                    or change_restores
                )
                if not keep_key:
                    if key in self._known_keys:
                        changed_any = True
                    self._snapshots.pop(key, None)
                    self._known_keys.discard(key)
                    self._observed_at_by_key.pop(key, None)
                    continue

                self._known_keys.add(key)
                self._observed_at_by_key[key] = latest_change_at
                if not depth:
                    if old is not None:
                        changed_any = True
                    self._snapshots.pop(key, None)
                    continue

                levels = tuple(
                    sorted(
                        depth.values(),
                        key=lambda level: level.price,
                        reverse=True,
                    )
                )
                snapshot = MarketSnapshot(
                    event_id=key[0],
                    market_id=key[1],
                    strike_id=key[2],
                    outcome=key[3],
                    levels=levels,
                    received_at_monotonic=latest_change_at,
                    received_at_wall=received_wall,
                    exchange_timestamp=old.exchange_timestamp if old else None,
                    sequence=old.sequence if old else None,
                    subtype=old.subtype if old else None,
                )
                if old != snapshot:
                    changed_any = True
                    changed.append(snapshot)
                self._snapshots[key] = snapshot

            for change_key in tuple(self._level_changes):
                if change_key[0][0] == event_id:
                    del self._level_changes[change_key]
            for key in tuple(self._membership_removals):
                if key[0] == event_id:
                    del self._membership_removals[key]
            for key in tuple(self._selection_resets):
                if key[0] == event_id:
                    del self._selection_resets[key]
            self._event_replace_at.pop(event_id, None)
            if changed_any:
                self._last_update_at_by_event[event_id] = received_mono
                self._condition.notify_all()
            callbacks = tuple(self._callbacks)
        for snapshot in changed:
            for callback in callbacks:
                callback(snapshot)
        return AuthoritativeApplyResult(accepted=True, changed=changed_any)

    def clear_event(self, event_id: int) -> int:
        """Remove all depth, tombstones, sequences, and seed state for an event."""

        if event_id <= 0:
            raise ValueError("event_id must be positive")
        with self._condition:
            keys = {key for key in self._known_keys if key[0] == event_id}
            for key in keys:
                self._snapshots.pop(key, None)
                self._known_keys.discard(key)
                self._observed_at_by_key.pop(key, None)
                self._selection_resets.pop(key, None)
                self._membership_removals.pop(key, None)
            for change_key in tuple(self._level_changes):
                if change_key[0][0] == event_id:
                    del self._level_changes[change_key]
            for key in tuple(self._membership_removals):
                if key[0] == event_id:
                    del self._membership_removals[key]
            for scope in tuple(self._last_sequence):
                if scope[0] == event_id:
                    del self._last_sequence[scope]
            self._reconcile_revision_by_event.pop(event_id, None)
            self._invalid_event_ids.discard(event_id)
            self._invalidation_epoch_by_event[event_id] = (
                self._invalidation_epoch_by_event.get(event_id, 0) + 1
            )
            self._event_replace_at.pop(event_id, None)
            for layout_key in tuple(self._selection_keys_by_market):
                if layout_key[0] == event_id:
                    del self._selection_keys_by_market[layout_key]
                    self._selection_layout_observed_at.pop(layout_key, None)
            self._last_update_at_by_event.pop(event_id, None)
            self._seeded_event_ids.discard(event_id)
            self._seeding_event_ids.discard(event_id)
            if keys:
                self._condition.notify_all()
            return len(keys)

    def snapshot(self, key: MarketKey) -> MarketSnapshot | None:
        with self._lock:
            if key[0] in self._invalid_event_ids:
                return None
            return self._snapshots.get(key)

    def current_levels(self, key: MarketKey) -> tuple[DepthLevel, ...]:
        snapshot = self.snapshot(key)
        return snapshot.levels if snapshot else ()

    def has_key(self, key: MarketKey) -> bool:
        """Return whether the cache has observed this selection, including deletes."""

        with self._lock:
            return key in self._known_keys

    def known_keys(self, *, event_id: int | None = None) -> frozenset[MarketKey]:
        with self._lock:
            return frozenset(
                key
                for key in self._known_keys
                if event_id is None or key[0] == event_id
            )

    def event_snapshot(self, event_id: int) -> EventDepthSnapshot:
        """Return known keys and live depth from one cache lock acquisition."""

        if event_id <= 0:
            raise ValueError("event_id must be positive")
        with self._lock:
            known_keys = frozenset(
                key for key in self._known_keys if key[0] == event_id
            )
            return EventDepthSnapshot(
                event_id=event_id,
                valid=event_id not in self._invalid_event_ids,
                known_keys=known_keys,
                snapshots={
                    key: snapshot
                    for key, snapshot in self._snapshots.items()
                    if key[0] == event_id
                },
            )

    def invalidate_events(self, event_ids: Iterable[int]) -> None:
        ids = tuple(dict.fromkeys(event_ids))
        if any(
            isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0
            for event_id in ids
        ):
            raise ValueError("event_ids must contain positive integers")
        with self._condition:
            for event_id in ids:
                self._invalidation_epoch_by_event[event_id] = (
                    self._invalidation_epoch_by_event.get(event_id, 0) + 1
                )
            self._invalid_event_ids.update(ids)
            self._condition.notify_all()

    def books_valid(self, event_ids: Iterable[int] | None = None) -> bool:
        with self._lock:
            if event_ids is None:
                return not self._invalid_event_ids
            return not any(
                event_id in self._invalid_event_ids for event_id in event_ids
            )

    def best_level(self, key: MarketKey) -> DepthLevel | None:
        levels = self.current_levels(key)
        return levels[0] if levels else None

    def snapshots(
        self,
        *,
        event_id: int | None = None,
        market_id: str | int | None = None,
    ) -> dict[MarketKey, MarketSnapshot]:
        market = str(market_id) if market_id is not None else None
        with self._lock:
            return {
                key: value
                for key, value in self._snapshots.items()
                if key[0] not in self._invalid_event_ids
                if (event_id is None or key[0] == event_id)
                and (market is None or key[1] == market)
            }

    def wait_for_update(
        self,
        *,
        after_monotonic: float,
        timeout: float | None = None,
        event_id: int | None = None,
    ) -> bool:
        def updated() -> bool:
            if event_id is not None:
                return (
                    self._last_update_at_by_event.get(event_id, 0.0) > after_monotonic
                )
            return any(
                updated_at > after_monotonic
                for updated_at in self._last_update_at_by_event.values()
            )

        with self._condition:
            return self._condition.wait_for(updated, timeout)

    def wait_ready(self, timeout: float | None = None) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: self._ready, timeout)

    def seed(self, client: Any, event_ids: Iterable[int]) -> bool:
        ids = tuple(dict.fromkeys(event_ids))
        with self._condition:
            self._condition.wait_for(
                lambda: not self._seeding_event_ids.intersection(ids)
            )
            missing = tuple(item for item in ids if item not in self._seeded_event_ids)
            self._seeding_event_ids.update(missing)
        if not missing:
            return False
        succeeded = False
        try:
            self.reconcile(client, missing)
            succeeded = True
        finally:
            with self._condition:
                self._seeding_event_ids.difference_update(missing)
                if succeeded:
                    self._seeded_event_ids.update(missing)
                self._condition.notify_all()
        return True

    def reconcile(self, client: Any, event_ids: Iterable[int]) -> int:
        ids = tuple(dict.fromkeys(event_ids))
        if not ids:
            return 0
        request = self.begin_reconcile(ids)
        markets_by_event = client.get_multiple_markets(ids)
        received_at_monotonic = self._monotonic()
        received_at_wall = self._wall_clock()
        updates = 0
        for event_id in ids:
            markets = markets_by_event.get(str(event_id), [])
            if self.apply_authoritative_snapshot(
                event_id,
                {"markets": markets},
                request=request,
                received_at_monotonic=received_at_monotonic,
                received_at_wall=received_at_wall,
            ):
                updates += 1
        return updates


def _prepare_group(
    key: MarketKey,
    raw_levels: list[dict[str, Any]],
    replace_levels: bool,
    *,
    strict: bool,
) -> _PreparedGroup:
    normalized = normalize_selection_levels(raw_levels, strict=strict)
    deletion_prices: list[int] = []
    for raw in raw_levels:
        quantity = raw.get("quantity")
        raw_value = raw.get("value")
        is_deletion = (
            isinstance(quantity, int | float)
            and not isinstance(quantity, bool)
            and float(quantity) <= 0
        ) or (
            quantity is None
            and (
                raw_value is None
                or (
                    isinstance(raw_value, int | float)
                    and not isinstance(raw_value, bool)
                    and float(raw_value) <= 0
                )
            )
        )
        if not is_deletion or raw.get("price") is None:
            continue
        price = raw["price"]
        if (
            isinstance(price, bool)
            or not isinstance(price, int | float)
            or not math.isfinite(float(price))
            or not float(price).is_integer()
        ):
            raise ValueError("selection deletion price must be a finite integer")
        deletion_prices.append(validate_american_odds(int(price)))
    return _PreparedGroup(
        key=key,
        replace_levels=replace_levels,
        normalized_levels=normalized,
        deletion_prices=tuple(deletion_prices),
        seen_prices=frozenset(
            {level.price for level in normalized} | set(deletion_prices)
        ),
    )


def _extract_sequence(payload: dict[str, Any]) -> int | None:
    for key in (
        "sequence_number",
        "sequence",
        "seq",
        "update_sequence",
        "version",
    ):
        value = payload.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    for key in ("data", "info", "meta"):
        value = payload.get(key)
        if isinstance(value, dict):
            found = _extract_sequence(value)
            if found is not None:
                return found
    return None


def _extract_groups(
    payload: dict[str, Any],
    default_event_id: int,
    *,
    force_replace: bool,
    selection_layout: dict[
        tuple[int, str],
        tuple[MarketKey | None, ...],
    ],
) -> list[tuple[MarketKey, list[dict[str, Any]], bool]]:
    groups: list[tuple[MarketKey, list[dict[str, Any]], bool]] = []

    def walk(
        value: Any,
        context: dict[str, Any],
        replace_hint: bool,
        *,
        preserve_market_id: bool = False,
    ) -> None:
        if isinstance(value, list):
            for item in value:
                walk(
                    item,
                    context,
                    replace_hint,
                    preserve_market_id=preserve_market_id,
                )
            return
        if not isinstance(value, dict):
            return
        current = dict(context)
        event = value.get("event_id", value.get("sport_event_id"))
        if isinstance(event, int) and event > 0:
            current["event_id"] = event
        looks_like_market = any(
            key in value for key in ("market_strikes", "selections", "sub_type")
        )
        if looks_like_market and not preserve_market_id and value.get("id") is not None:
            current["market_id"] = value["id"]
        if value.get("market_id") is not None:
            current["market_id"] = value["market_id"]
        if value.get("strike_id") is not None:
            current["strike_id"] = value["strike_id"]
        if value.get("outcome_id") is not None:
            current["outcome"] = value["outcome_id"]
        elif value.get("name") is not None and not looks_like_market:
            current["outcome"] = value["name"]

        if _looks_like_level(value):
            add_levels([value], current, replace_hint)
            return

        for key, child in value.items():
            if key == "markets":
                walk(child, current, True)
            elif key == "market_strikes":
                walk(child, current, True, preserve_market_id=True)
            elif key == "market_selections":
                add_container(child, current, False)
            elif key == "selections":
                add_container(child, current, True)
            elif key in {
                "data",
                "info",
                "payload",
                "updates",
                "update",
                "result",
            }:
                walk(child, current, replace_hint)

    def add_container(value: Any, context: dict[str, Any], replace_hint: bool) -> None:
        if not isinstance(value, list):
            walk(value, context, replace_hint)
            return
        if not value:
            key = _key_from_context(context)
            if key is not None:
                groups.append((key, [], force_replace or replace_hint))
            return
        if value and all(isinstance(item, list) for item in value):
            event_id = context.get("event_id")
            market_id = context.get("market_id")
            known_layout = (
                selection_layout.get((event_id, str(market_id)), ())
                if isinstance(event_id, int) and market_id is not None
                else ()
            )
            for index, item in enumerate(value):
                if item:
                    add_levels(item, context, True)
                elif index < len(known_layout):
                    known_key = known_layout[index]
                    if known_key is not None:
                        groups.append((known_key, [], True))
            return
        if all(isinstance(item, dict) for item in value):
            grouped: dict[MarketKey, list[dict[str, Any]]] = defaultdict(list)
            for item in value:
                if _looks_like_level(item):
                    key = _key_for_level(item, context)
                    if key is not None:
                        grouped[key].append(item)
                else:
                    walk(item, context, replace_hint)
            for key, levels in grouped.items():
                groups.append((key, levels, force_replace or replace_hint))
            return
        walk(value, context, replace_hint)

    def add_levels(
        levels: Iterable[Any], context: dict[str, Any], replace_hint: bool
    ) -> None:
        grouped: dict[MarketKey, list[dict[str, Any]]] = defaultdict(list)
        for item in levels:
            if not isinstance(item, dict):
                continue
            key = _key_for_level(item, context)
            if key is not None:
                grouped[key].append(item)
        for key, values in grouped.items():
            groups.append((key, values, force_replace or replace_hint))

    walk(payload, {"event_id": default_event_id}, force_replace)
    return groups


def _extract_selection_layout(
    payload: dict[str, Any],
    default_event_id: int,
    existing: dict[tuple[int, str], tuple[MarketKey | None, ...]],
) -> dict[tuple[int, str], tuple[MarketKey | None, ...]]:
    layouts: dict[tuple[int, str], tuple[MarketKey | None, ...]] = {}

    def walk(
        value: Any,
        context: dict[str, Any],
        *,
        preserve_market_id: bool = False,
    ) -> None:
        if isinstance(value, list):
            for item in value:
                walk(item, context, preserve_market_id=preserve_market_id)
            return
        if not isinstance(value, dict):
            return
        current = dict(context)
        event_id = value.get("event_id", value.get("sport_event_id"))
        if isinstance(event_id, int) and event_id > 0:
            current["event_id"] = event_id
        looks_like_market = any(
            key in value for key in ("market_strikes", "selections", "sub_type")
        )
        if looks_like_market and not preserve_market_id and value.get("id") is not None:
            current["market_id"] = value["id"]
        if value.get("market_id") is not None:
            current["market_id"] = value["market_id"]
        if value.get("strike_id") is not None:
            current["strike_id"] = value["strike_id"]

        selections = value.get("selections")
        event = current.get("event_id")
        market = current.get("market_id")
        if (
            isinstance(selections, list)
            and isinstance(event, int)
            and market is not None
        ):
            layout_key = (event, str(market))
            previous = existing.get(layout_key, ())
            keys: list[MarketKey | None] = []
            for index, levels in enumerate(selections):
                key = None
                if isinstance(levels, list):
                    key = next(
                        (
                            _key_for_level(level, current)
                            for level in levels
                            if isinstance(level, dict)
                            and _key_for_level(level, current) is not None
                        ),
                        None,
                    )
                if key is None and index < len(previous):
                    key = previous[index]
                keys.append(key)
            layouts[layout_key] = tuple(keys)

        for key, child in value.items():
            if key == "market_strikes":
                walk(child, current, preserve_market_id=True)
            elif key in {
                "data",
                "info",
                "markets",
                "payload",
                "updates",
                "update",
                "result",
            }:
                walk(child, current)

    walk(payload, {"event_id": default_event_id})
    return layouts


def _looks_like_level(value: dict[str, Any]) -> bool:
    return "price" in value and (
        "quantity" in value
        or "value" in value
        or "outcome_id" in value
        or "strike_id" in value
    )


def _key_for_level(level: dict[str, Any], context: dict[str, Any]) -> MarketKey | None:
    event_id = level.get(
        "event_id", level.get("sport_event_id", context.get("event_id"))
    )
    market_id = level.get("market_id", context.get("market_id"))
    strike_id = level.get("strike_id", context.get("strike_id"))
    outcome = level.get("outcome_id")
    if outcome is None:
        outcome = level.get("name", level.get("display_name", context.get("outcome")))
    if (
        not isinstance(event_id, int)
        or event_id <= 0
        or market_id is None
        or strike_id is None
        or outcome is None
    ):
        return None
    return event_id, str(market_id), str(strike_id), str(outcome)


def _key_from_context(context: dict[str, Any]) -> MarketKey | None:
    event_id = context.get("event_id")
    market_id = context.get("market_id")
    strike_id = context.get("strike_id")
    outcome = context.get("outcome")
    if (
        not isinstance(event_id, int)
        or event_id <= 0
        or market_id is None
        or strike_id is None
        or outcome is None
    ):
        return None
    return event_id, str(market_id), str(strike_id), str(outcome)
