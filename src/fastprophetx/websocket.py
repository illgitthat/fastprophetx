"""One-socket threaded ProphetX/Pusher market stream."""

from __future__ import annotations

import base64
import binascii
import random
import re
import threading
import time
from collections.abc import Callable, Iterable
from contextlib import nullcontext, suppress
from typing import Any
from urllib.parse import quote

import orjson
from websockets.sync.client import connect

from .cache import MarketCache
from .errors import ResponseError
from .protocol import canonicalize_subtype

_EVENT_RE = re.compile(r"(?:^|[-_:])event[=:_-]?(\d+)(?:[-_:]|$)", re.IGNORECASE)
_SUBTYPE_RE = re.compile(
    r"(?:sub[_-]?type|subtype)[=:_-]?([A-Za-z0-9_]+)", re.IGNORECASE
)
_MAX_RECONNECT_EXPONENT = 30
_STRUCTURAL_CHANGE_TYPES = frozenset({"market", "market_strike", "sport_event"})
_ORDER_EVENT_NAMES = frozenset({"order", "orders"})


class ProphetXWebSocket:
    """Maintain one synchronous WebSocket for many exact channel scopes."""

    def __init__(
        self,
        client: Any,
        *,
        cache: MarketCache | None = None,
        on_update: Callable[
            [
                int,
                str | None,
                dict[str, Any],
                int | float | None,
                float,
                int,
            ],
            None,
        ]
        | None = None,
        on_order_update: Callable[
            [
                dict[str, Any],
                int | float | None,
                float,
                int,
            ],
            None,
        ]
        | None = None,
        connector: Callable[..., Any] = connect,
        health_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        reconnect_base: float = 0.25,
        reconnect_cap: float = 8.0,
        random_source: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client = client
        self.cache = cache or MarketCache()
        self._on_update = on_update
        self._on_order_update = on_order_update
        self._connector = connector
        self._health_timeout = health_timeout
        self._connect_timeout = connect_timeout
        self._reconnect_base = reconnect_base
        self._reconnect_cap = reconnect_cap
        self._random = random_source
        self._monotonic = monotonic
        self._lifecycle_lock = threading.Lock()
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._worker_token: int | None = None
        self._worker_cancel: threading.Event | None = None
        self._next_worker_token = 0
        self._event_ids: tuple[int, ...] = ()
        self._event_subtype_pairs: tuple[tuple[int, str], ...] = ()
        self._generation = 0
        self._ready_count = 0
        self._socket: Any = None
        self._socket_owner_token: int | None = None

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def event_ids(self) -> tuple[int, ...]:
        with self._lock:
            return self._event_ids

    @property
    def event_subtype_pairs(self) -> tuple[tuple[int, str], ...]:
        with self._lock:
            return self._event_subtype_pairs

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def books_ready(self, event_ids: Iterable[int] | None = None) -> bool:
        return self.ready and self.cache.books_valid(event_ids)

    def start(
        self,
        event_ids: Iterable[int] = (),
        event_subtype_pairs: Iterable[tuple[int, str]] = (),
    ) -> int:
        with self._lifecycle_lock:
            generation = self.set_subscriptions(
                event_ids,
                event_subtype_pairs,
            )
            with self._lock:
                if (
                    self._thread is not None
                    and self._thread.is_alive()
                    and self._worker_cancel is not None
                    and not self._worker_cancel.is_set()
                ):
                    return generation
                self._stop.clear()
                self._next_worker_token += 1
                worker_token = self._next_worker_token
                worker_cancel = threading.Event()
                self._worker_token = worker_token
                self._worker_cancel = worker_cancel
                self._thread = threading.Thread(
                    target=self._run,
                    args=(worker_token, worker_cancel),
                    name="fastprophetx-websocket",
                    daemon=True,
                )
                self._thread.start()
                return generation

    def set_subscriptions(
        self,
        event_ids: Iterable[int] = (),
        event_subtype_pairs: Iterable[tuple[int, str]] = (),
    ) -> int:
        ids = tuple(sorted(set(event_ids)))
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in ids
        ):
            raise ValueError("event IDs must be positive integers")
        pairs = tuple(
            sorted(
                {
                    (event_id, canonicalize_subtype(subtype))
                    for event_id, subtype in event_subtype_pairs
                }
            )
        )
        if any(
            isinstance(event_id, bool)
            or not isinstance(event_id, int)
            or event_id <= 0
            or not subtype
            for event_id, subtype in pairs
        ):
            raise ValueError(
                "event subtype pairs must contain a positive ID and subtype"
            )
        if not ids and not pairs:
            raise ValueError("at least one websocket subscription is required")
        with self._lock:
            if ids == self._event_ids and pairs == self._event_subtype_pairs:
                return self._generation
            previous_ids = self._event_ids
            previous_pairs = self._event_subtype_pairs
            self._event_ids = ids
            self._event_subtype_pairs = pairs
            self._generation += 1
            generation = self._generation
            self._ready.clear()
            self.cache.set_connection_state(
                connected=self.cache.connected,
                ready=False,
            )
            self.cache.invalidate_events(
                _subscription_event_ids(
                    (*previous_ids, *ids),
                    (*previous_pairs, *pairs),
                )
            )
            socket = self._socket
        if socket is not None:
            with suppress(Exception):
                socket.close()
        return generation

    def wait_ready(self, timeout: float | None = None) -> bool:
        return self._ready.wait(timeout)

    def close(self) -> None:
        with self._lifecycle_lock, self._lock:
            self._stop.set()
            self._generation += 1
            close_generation = self._generation
            self._ready.clear()
            socket = self._socket
            thread = self._thread
            worker_token = self._worker_token
            worker_cancel = self._worker_cancel
            if worker_cancel is not None:
                worker_cancel.set()
            event_ids = self._event_ids
            pairs = self._event_subtype_pairs
            self.cache.invalidate_events(_subscription_event_ids(event_ids, pairs))
        if socket is not None:
            with suppress(Exception):
                socket.close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(2.0, self._connect_timeout + 1.0))
        with self._lock:
            if self._worker_token == worker_token and (
                thread is None or not thread.is_alive()
            ):
                self._thread = None
                self._worker_token = None
                self._worker_cancel = None
            if self._generation == close_generation and self._stop.is_set():
                self.cache.set_connection_state(
                    connected=False,
                    ready=False,
                )

    def _snapshot(
        self,
    ) -> tuple[tuple[int, ...], tuple[tuple[int, str], ...], int]:
        with self._lock:
            return self._event_ids, self._event_subtype_pairs, self._generation

    def _run(
        self,
        worker_token: int,
        worker_cancel: threading.Event,
    ) -> None:
        attempt = 0
        while not worker_cancel.is_set():
            event_ids, pairs, generation = self._snapshot()
            with self._lock:
                ready_count = self._ready_count
            try:
                self._connect_once(
                    event_ids,
                    pairs,
                    generation,
                    worker_token,
                    worker_cancel,
                )
                attempt = 0
            except Exception as exc:
                with self._lock:
                    if (
                        worker_cancel.is_set()
                        or self._worker_token != worker_token
                        or self._worker_cancel is not worker_cancel
                    ):
                        break
                    if self._ready_count != ready_count:
                        attempt = 0
                    self._ready.clear()
                    if generation == self._generation:
                        self.cache.invalidate_events(
                            _subscription_event_ids(event_ids, pairs)
                        )
                    self.cache.set_connection_state(
                        connected=False,
                        ready=False,
                        error=str(exc),
                    )
                delay = min(
                    self._reconnect_cap,
                    self._reconnect_base
                    * (2.0 ** min(attempt, _MAX_RECONNECT_EXPONENT)),
                )
                delay *= 0.5 + self._random() * 0.5
                attempt = min(attempt + 1, _MAX_RECONNECT_EXPONENT)
                worker_cancel.wait(delay)
        with self._lock:
            if self._worker_token == worker_token:
                self.cache.set_connection_state(connected=False, ready=False)

    def _connect_once(
        self,
        event_ids: tuple[int, ...],
        pairs: tuple[tuple[int, str], ...],
        generation: int,
        worker_token: int,
        worker_cancel: threading.Event,
    ) -> None:
        config = self.client.get_websocket_connection_config()
        key = config.get("key")
        ws_host = config.get("ws_host")
        if not isinstance(ws_host, str) or not ws_host:
            cluster = config.get("cluster")
            if not isinstance(cluster, str) or not cluster:
                raise ResponseError("websocket config omitted ws_host and cluster")
            ws_host = f"ws-{cluster}.pusher.com"
        if not isinstance(key, str) or not key:
            raise ResponseError("websocket config omitted key")
        url = (
            f"wss://{ws_host}/app/{quote(key, safe='')}"
            "?protocol=7&client=fastprophetx&version=0.1.0&flash=false"
        )
        socket = self._connector(
            url,
            open_timeout=self._connect_timeout,
            ping_interval=None,
        )
        socket_context = socket if hasattr(socket, "__enter__") else nullcontext(socket)
        with socket_context as ws:
            with self._lock:
                if not self._is_current_generation(
                    generation,
                    worker_token,
                    worker_cancel,
                ):
                    return
                self._socket = ws
                self._socket_owner_token = worker_token
                self.cache.set_connection_state(connected=True, ready=False)
            try:
                self._establish(
                    ws,
                    event_ids,
                    pairs,
                    generation,
                    worker_token=worker_token,
                    worker_cancel=worker_cancel,
                )
            finally:
                with self._lock:
                    if (
                        self._worker_token == worker_token
                        and self._socket_owner_token == worker_token
                        and self._socket is ws
                    ):
                        self._ready.clear()
                        self._socket = None
                        self._socket_owner_token = None

    def _establish(
        self,
        ws: Any,
        event_ids: tuple[int, ...],
        pairs: tuple[tuple[int, str], ...],
        generation: int,
        *,
        worker_token: int | None = None,
        worker_cancel: threading.Event | None = None,
    ) -> None:
        raw_frame = ws.recv(timeout=self._connect_timeout)
        if not self._is_current_generation(
            generation,
            worker_token,
            worker_cancel,
        ):
            return
        frame = _decode_frame(raw_frame)
        if frame.get("event") != "pusher:connection_established":
            raise ResponseError("expected pusher:connection_established")
        connection_data = _decode_json(frame.get("data"))
        socket_id = (
            connection_data.get("socket_id")
            if isinstance(connection_data, dict)
            else None
        )
        if not isinstance(socket_id, str) or not socket_id:
            raise ResponseError("connection frame omitted socket_id")
        registration = self.client.register_websocket(socket_id, event_ids, pairs)
        scopes, channels, rejected = _registration_state(registration, event_ids, pairs)

        authenticated = registration.get("authenticated")
        if isinstance(authenticated, dict):
            ws.send(_encode({"event": "pusher:signin", "data": authenticated}))
            if not self._wait_for_signin(
                ws,
                generation,
                worker_token,
                worker_cancel,
            ):
                return

        market_events: set[str] = set()
        health_events: set[str] = set()
        order_channels: set[str] = set()
        scope_by_channel: dict[str, tuple[int, str | None]] = {}
        expected_channels: set[str] = set()
        for channel in channels:
            name = channel["channel_name"]
            auth = channel["auth"]
            scope = channel.get("scope")
            if isinstance(scope, dict) and isinstance(scope.get("event_id"), int):
                scope_by_channel[name] = (
                    scope["event_id"],
                    canonicalize_subtype(scope["sub_type"])
                    if isinstance(scope.get("sub_type"), str)
                    and scope["sub_type"].strip()
                    else None,
                )
            for binding in channel.get("binding_events", []):
                event_name = binding.get("name") if isinstance(binding, dict) else None
                if not isinstance(event_name, str) or not event_name:
                    continue
                if "health" in event_name.lower():
                    health_events.add(event_name)
                elif scope is None and event_name in _ORDER_EVENT_NAMES:
                    order_channels.add(name)
                else:
                    market_events.add(event_name)
            expected_channels.add(name)
            ws.send(
                _encode(
                    {
                        "event": "pusher:subscribe",
                        "data": {"auth": auth, "channel": name},
                    }
                )
            )

        subscribed: set[str] = set()
        deadline = self._monotonic() + self._connect_timeout
        while expected_channels - subscribed:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError("subscription acknowledgements timed out")
            try:
                raw_frame = ws.recv(timeout=min(remaining, 1.0))
            except TimeoutError:
                if not self._is_current_generation(
                    generation,
                    worker_token,
                    worker_cancel,
                ):
                    return
                continue
            if not self._is_current_generation(
                generation,
                worker_token,
                worker_cancel,
            ):
                return
            frame = _decode_frame(raw_frame)
            if self._handle_control(ws, frame):
                continue
            if frame.get("event") == "pusher_internal:subscription_succeeded":
                channel = frame.get("channel")
                if isinstance(channel, str):
                    subscribed.add(channel)
            self._handle_update(
                frame,
                market_events,
                scope_by_channel,
                generation,
                order_channels=order_channels,
                worker_token=worker_token,
                worker_cancel=worker_cancel,
            )

        with self._lock:
            if not self._is_current_generation(
                generation,
                worker_token,
                worker_cancel,
            ):
                return
            self.cache.set_connection_state(
                connected=True,
                ready=True,
                authorized_scopes=scopes,
                rejected=rejected,
            )
            self._ready.set()
            self._ready_count += 1
        last_transport_activity = self._monotonic()
        last_health_activity = last_transport_activity
        while not self._cancelled(worker_cancel):
            _, _, current_generation = self._snapshot()
            if current_generation != generation:
                return
            now = self._monotonic()
            last_expected_activity = (
                last_health_activity if health_events else last_transport_activity
            )
            remaining_health = self._health_timeout - (now - last_expected_activity)
            if remaining_health <= 0:
                if health_events:
                    raise TimeoutError("private health_check timed out")
                raise TimeoutError("websocket transport health timeout")
            try:
                raw_frame = ws.recv(timeout=min(1.0, remaining_health))
            except TimeoutError:
                continue
            if not self._is_current_generation(
                generation,
                worker_token,
                worker_cancel,
            ):
                return
            frame = _decode_frame(raw_frame)
            last_transport_activity = self._monotonic()
            if self._handle_control(ws, frame):
                continue
            if frame.get("event") in health_events:
                last_health_activity = last_transport_activity
                continue
            self._handle_update(
                frame,
                market_events,
                scope_by_channel,
                generation,
                order_channels=order_channels,
                worker_token=worker_token,
                worker_cancel=worker_cancel,
            )

    def _wait_for_signin(
        self,
        ws: Any,
        generation: int,
        worker_token: int | None,
        worker_cancel: threading.Event | None,
    ) -> bool:
        deadline = self._monotonic() + self._connect_timeout
        while True:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError("websocket signin timed out")
            try:
                raw_frame = ws.recv(timeout=min(remaining, 1.0))
            except TimeoutError:
                if not self._is_current_generation(
                    generation,
                    worker_token,
                    worker_cancel,
                ):
                    return False
                continue
            if not self._is_current_generation(
                generation,
                worker_token,
                worker_cancel,
            ):
                return False
            frame = _decode_frame(raw_frame)
            if self._handle_control(ws, frame):
                continue
            if frame.get("event") == "pusher:signin_success":
                return True

    def _is_current_generation(
        self,
        generation: int,
        worker_token: int | None = None,
        worker_cancel: threading.Event | None = None,
    ) -> bool:
        with self._lock:
            if worker_token is None:
                return not self._stop.is_set() and generation == self._generation
            return self._worker_is_current(
                worker_token,
                worker_cancel,
                generation,
            )

    def _worker_is_current(
        self,
        worker_token: int | None,
        worker_cancel: threading.Event | None,
        generation: int,
    ) -> bool:
        return (
            worker_token is not None
            and worker_cancel is not None
            and not worker_cancel.is_set()
            and self._worker_token == worker_token
            and self._worker_cancel is worker_cancel
            and generation == self._generation
        )

    def _cancelled(self, worker_cancel: threading.Event | None) -> bool:
        return self._stop.is_set() if worker_cancel is None else worker_cancel.is_set()

    def _invalidate_generation_books(
        self,
        generation: int,
        event_ids: Iterable[int],
        pairs: Iterable[tuple[int, str]],
        *,
        worker_token: int | None = None,
        worker_cancel: threading.Event | None = None,
    ) -> bool:
        with self._lock:
            if worker_token is None:
                current = generation == self._generation
            else:
                current = self._worker_is_current(
                    worker_token,
                    worker_cancel,
                    generation,
                )
            if not current:
                return False
            self.cache.invalidate_events(_subscription_event_ids(event_ids, pairs))
            return True

    @staticmethod
    def _handle_control(ws: Any, frame: dict[str, Any]) -> bool:
        event = frame.get("event")
        if event == "pusher:error":
            raise ResponseError(
                "Pusher returned an error", details=_decode_json(frame.get("data"))
            )
        if event == "pusher:ping":
            ws.send(_encode({"event": "pusher:pong", "data": {}}))
            return True
        return event == "pusher:pong"

    def _handle_update(
        self,
        frame: dict[str, Any],
        market_events: set[str],
        scope_by_channel: dict[str, tuple[int, str | None]],
        connection_generation: int = 0,
        *,
        order_channels: set[str] | None = None,
        worker_token: int | None = None,
        worker_cancel: threading.Event | None = None,
    ) -> bool:
        callback: Callable[..., None] | None = None
        callback_args: tuple[Any, ...] | None = None
        handled = False
        with self._lock:
            if not self._is_current_generation(
                connection_generation,
                worker_token,
                worker_cancel,
            ):
                return False
            channel = frame.get("channel")
            if not isinstance(channel, str):
                return False
            event_name = frame.get("event")
            is_order_update = bool(
                event_name in _ORDER_EVENT_NAMES
                and order_channels is not None
                and channel in order_channels
            )
            if (
                not is_order_update
                and market_events
                and event_name not in market_events
            ):
                return False
            data = _decode_json(frame.get("data"))
            if not isinstance(data, dict):
                return False
            payload = _decode_market_payload(data.get("payload", data.get("data")))
            if not isinstance(payload, dict):
                return False
            change_type = data.get("change_type")
            if not isinstance(change_type, str) or not change_type:
                change_type = event_name if isinstance(event_name, str) else None
            operation = data.get("op")
            timestamp = data.get("timestamp")
            exchange_timestamp = (
                timestamp
                if isinstance(timestamp, int | float)
                and not isinstance(timestamp, bool)
                else None
            )
            received = self._monotonic()
            if is_order_update:
                handled = True
                if self._on_order_update is not None:
                    callback = self._on_order_update
                    callback_args = (
                        payload,
                        exchange_timestamp,
                        received,
                        connection_generation,
                    )
            else:
                scope = scope_by_channel.get(channel) or _scope_from_channel(channel)
                event_id = (
                    scope[0] if scope is not None else _event_id_from_payload(payload)
                )
                if isinstance(event_id, int) and (
                    change_type in _STRUCTURAL_CHANGE_TYPES or operation == "d"
                ):
                    self.cache.invalidate_events((event_id,))
                    return True
                if scope is None:
                    return False
                changed = self.cache.apply_update(
                    scope[0],
                    payload,
                    subtype=scope[1],
                    sequence=_message_sequence(payload, exchange_timestamp),
                    exchange_timestamp=exchange_timestamp,
                    received_at_monotonic=received,
                )
                handled = changed
                if changed and self._on_update is not None:
                    callback = self._on_update
                    callback_args = (
                        scope[0],
                        scope[1],
                        payload,
                        exchange_timestamp,
                        received,
                        connection_generation,
                    )
        if callback is not None and callback_args is not None:
            callback(*callback_args)
        return handled


def _registration_state(
    data: dict[str, Any],
    event_ids: tuple[int, ...],
    pairs: tuple[tuple[int, str], ...],
) -> tuple[
    frozenset[tuple[int, str | None]], list[dict[str, Any]], list[dict[str, Any]]
]:
    if data.get("success") is not True:
        raise ResponseError("websocket registration failed", raw_payload=data)
    status = data.get("status")
    if not isinstance(status, str) or status.lower() not in {
        "connected",
        "success",
        "ok",
        "authenticated",
        "registered",
    }:
        raise ResponseError("websocket registration status was unsuccessful")
    channels = data.get("authorized_channel")
    rejected = data.get("rejected")
    if not isinstance(channels, list) or not all(
        isinstance(channel, dict) for channel in channels
    ):
        raise ResponseError("registration omitted authorized_channel")
    if rejected is None:
        rejected = []
    if not isinstance(rejected, list) or not all(
        isinstance(item, dict) for item in rejected
    ):
        raise ResponseError("registration omitted rejected")
    if rejected:
        raise ResponseError("registration rejected subscriptions", details=rejected)
    count = data.get("channel_count")
    limit = data.get("channel_limit")
    expected_count = len(event_ids) + len(pairs)
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count != expected_count
        or isinstance(limit, bool)
        or not isinstance(limit, int)
        or count > limit
    ):
        raise ResponseError("registration channel counts were invalid")
    scopes: set[tuple[int, str | None]] = set()
    valid_channels: list[dict[str, Any]] = []
    for channel in channels:
        name = channel.get("channel_name")
        auth = channel.get("auth")
        if not isinstance(name, str) or not name:
            raise ResponseError("authorized channel omitted channel_name")
        if not isinstance(auth, str) or not auth:
            raise ResponseError("authorized channel omitted auth")
        scope = channel.get("scope")
        if isinstance(scope, dict) and isinstance(scope.get("event_id"), int):
            scopes.add(
                (
                    scope["event_id"],
                    canonicalize_subtype(scope["sub_type"])
                    if isinstance(scope.get("sub_type"), str)
                    and scope["sub_type"].strip()
                    else None,
                )
            )
        valid_channels.append(channel)
    missing = {(event_id, None) for event_id in event_ids} - scopes
    missing_pairs = set(pairs) - scopes
    if missing or missing_pairs:
        raise ResponseError(
            "registration omitted required scopes",
            details={
                "missing_events": sorted(missing),
                "missing_event_subtypes": sorted(missing_pairs),
            },
        )
    return frozenset(scopes), valid_channels, rejected


def _decode_frame(raw: str | bytes) -> dict[str, Any]:
    try:
        value = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise ResponseError("websocket frame was invalid JSON") from exc
    if not isinstance(value, dict):
        raise ResponseError("websocket frame was not an object")
    return value


def _decode_json(value: Any) -> Any:
    for _ in range(3):
        if not isinstance(value, str | bytes):
            return value
        try:
            value = orjson.loads(value)
        except orjson.JSONDecodeError:
            return value
    return value


def _decode_market_payload(value: Any) -> Any:
    if isinstance(value, str):
        if value.lstrip().startswith(("{", "[", '"')):
            return _decode_json(value)
        try:
            value = base64.b64decode(value, validate=True)
        except binascii.Error, ValueError:
            return _decode_json(value)
    elif isinstance(value, bytes):
        if value.lstrip().startswith((b"{", b"[", b'"')):
            return _decode_json(value)
        with suppress(binascii.Error, ValueError):
            value = base64.b64decode(value, validate=True)
    return _decode_json(value)


def _scope_from_channel(channel: str) -> tuple[int, str | None] | None:
    event_match = _EVENT_RE.search(channel)
    if event_match is None:
        return None
    subtype_match = _SUBTYPE_RE.search(channel)
    return (
        int(event_match.group(1)),
        canonicalize_subtype(subtype_match.group(1)) if subtype_match else None,
    )


def _subscription_event_ids(
    event_ids: Iterable[int],
    pairs: Iterable[tuple[int, str]],
) -> tuple[int, ...]:
    return tuple(
        dict.fromkeys(
            (
                *event_ids,
                *(event_id for event_id, _subtype in pairs),
            )
        )
    )


def _message_sequence(
    payload: dict[str, Any], exchange_timestamp: int | float | None
) -> int | None:
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
    info = payload.get("info")
    if isinstance(info, dict):
        nested = _message_sequence(info, None)
        if nested is not None:
            return nested
    return (
        exchange_timestamp
        if isinstance(exchange_timestamp, int)
        and not isinstance(exchange_timestamp, bool)
        else None
    )


def _event_id_from_payload(payload: dict[str, Any]) -> int | None:
    for key in ("event_id", "sport_event_id"):
        value = payload.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
    for key in ("data", "info", "payload"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            event_id = _event_id_from_payload(nested)
            if event_id is not None:
                return event_id
    return None


def _encode(value: Any) -> str:
    return orjson.dumps(value).decode()
