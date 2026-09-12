from __future__ import annotations

import base64
import threading
from collections.abc import Callable, Sequence
from typing import Any

import orjson
import pytest

from fastprophetx import MarketCache, ProphetXWebSocket


def _frame(event: str, data: Any, channel: str | None = None) -> str:
    value = {"event": event, "data": data}
    if channel:
        value["channel"] = channel
    return orjson.dumps(value).decode()


def _channel(event_id: int, subtype: str | None = None) -> dict[str, Any]:
    name = f"private-event={event_id}-{subtype or 'all'}"
    scope: dict[str, Any] = {"event_id": event_id}
    if subtype:
        scope["sub_type"] = subtype
    return {
        "channel_name": name,
        "auth": f"auth-{event_id}-{subtype}",
        "binding_events": [{"name": "market_selections"}],
        "scope": scope,
    }


def _global_channel(name: str) -> dict[str, Any]:
    return {
        "channel_name": name,
        "auth": "global-auth",
        "binding_events": [{"name": "health_check"}],
    }


def _private_user_channel() -> dict[str, Any]:
    return {
        "channel_name": "private-user",
        "auth": "private-auth",
        "binding_events": [
            {"name": "orders"},
            {"name": "health_check"},
        ],
    }


class FakeSocket:
    def __init__(self, frames: Sequence[str | BaseException]) -> None:
        self.frames = list(frames)
        self.sent: list[dict[str, Any]] = []
        self.on_last_recv: Callable[[], None] | None = None

    def recv(self, timeout: float | None = None) -> str:
        del timeout
        if not self.frames:
            if self.on_last_recv is not None:
                self.on_last_recv()
                raise TimeoutError
            raise IndexError("no scripted websocket frames remain")
        value = self.frames.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    def send(self, value: str) -> None:
        self.sent.append(orjson.loads(value))

    def close(self) -> None:
        pass


class FakeClient:
    def __init__(self, registration: dict[str, Any]) -> None:
        self.registration = registration
        self.calls: list[tuple[str, tuple[int, ...], tuple[tuple[int, str], ...]]] = []

    def get_websocket_connection_config(self) -> dict[str, Any]:
        return {"key": "app-key", "ws_host": "dynamic.example"}

    def register_websocket(
        self,
        socket_id: str,
        event_ids: tuple[int, ...],
        pairs: tuple[tuple[int, str], ...],
    ) -> dict[str, Any]:
        self.calls.append((socket_id, event_ids, pairs))
        return self.registration


def test_multi_event_single_socket_and_double_json_base64_update() -> None:
    channels = [
        _global_channel("private-user"),
        _global_channel("private-tournaments"),
        _channel(1),
        _channel(2),
        _channel(1, "touchdown"),
    ]
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authenticated": {"auth": "signin", "user_data": "{}"},
        "authorized_channel": channels,
        "channel_count": 3,
        "channel_limit": 100,
        "rejected": [],
        "subscriptions": [],
    }
    payload = {
        "sport_event_id": 1,
        "market_id": 10,
        "info": {
            "sequence_number": 9,
            "selections": [
                [
                    {
                        "strike_id": "s",
                        "outcome_id": 8,
                        "price": 120,
                        "quantity": 3,
                    }
                ]
            ],
        },
    }
    encoded = base64.b64encode(orjson.dumps(orjson.dumps(payload).decode())).decode()
    socket = FakeSocket(
        [
            _frame(
                "pusher:connection_established",
                orjson.dumps({"socket_id": "1.2"}).decode(),
            ),
            _frame("pusher:signin_success", "{}"),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[0]["channel_name"],
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[1]["channel_name"],
            ),
            _frame(
                "market_selections",
                orjson.dumps(
                    {
                        "payload": encoded,
                        "timestamp": 123,
                        "change_type": "market_selections",
                        "op": "u",
                    }
                ).decode(),
                channels[2]["channel_name"],
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[2]["channel_name"],
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[3]["channel_name"],
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[4]["channel_name"],
            ),
        ]
    )
    cache = MarketCache()
    client = FakeClient(registration)
    updates: list[tuple[int, int]] = []

    def on_update(
        event_id: int,
        _subtype: str | None,
        _payload: dict[str, Any],
        _exchange_timestamp: int | float | None,
        _received: float,
        generation: int,
    ) -> None:
        updates.append((event_id, generation))

    stream = ProphetXWebSocket(client, cache=cache, on_update=on_update)
    socket.on_last_recv = stream._stop.set
    generation = stream.set_subscriptions((1, 2), ((1, "touchdown"),))
    request = cache.begin_reconcile((1, 2))
    for event_id in (1, 2):
        cache.apply_authoritative_snapshot(
            event_id,
            {"markets": []},
            request=request,
        )
    stream._establish(socket, (1, 2), ((1, "touchdown"),), generation)

    assert client.calls == [("1.2", (1, 2), ((1, "touchdown"),))]
    subscriptions = [
        item for item in socket.sent if item["event"] == "pusher:subscribe"
    ]
    assert len(subscriptions) == 5
    snapshot = cache.snapshot((1, "10", "s", "8"))
    assert snapshot is not None
    assert snapshot.sequence == 9
    assert snapshot.exchange_timestamp == 123
    assert cache.authorized_scopes == frozenset(
        {(1, None), (2, None), (1, "touchdown")}
    )
    assert updates == [(1, generation)]

    removal = {
        "sport_event_id": 1,
        "market_id": 10,
        "info": {
            "sequence_number": 10,
            "selections": [[]],
        },
    }
    removal_payload = base64.b64encode(
        orjson.dumps(orjson.dumps(removal).decode())
    ).decode()
    stream._stop.clear()
    stream._handle_update(
        _decode(
            _frame(
                "market_selections",
                orjson.dumps({"payload": removal_payload}).decode(),
                channels[2]["channel_name"],
            )
        ),
        {"market_selections"},
        {channels[2]["channel_name"]: (1, None)},
        generation,
    )
    assert cache.snapshot((1, "10", "s", "8")) is None


def test_private_order_update_is_delivered_outside_market_cache() -> None:
    callback_updates: list[tuple[dict[str, Any], int | float | None, float, int]] = []
    clock = [12.5]

    def on_order_update(
        payload: dict[str, Any],
        exchange_timestamp: int | float | None,
        received_at: float,
        generation: int,
    ) -> None:
        callback_updates.append((payload, exchange_timestamp, received_at, generation))

    order = {
        "info": {
            "order_id": "order-1",
            "external_id": "external-1",
            "filled_quantity": 4.96,
            "fill_price": -430,
            "total_filled_quantity": 4.96,
            "open_quantity": 0,
            "matching_status": "fully_matched",
            "status": "open",
        }
    }
    encoded = base64.b64encode(orjson.dumps(order)).decode()
    stream = ProphetXWebSocket(
        FakeClient({}),
        on_order_update=on_order_update,
        monotonic=lambda: clock[0],
    )

    handled = stream._handle_update(
        _decode(
            _frame(
                "orders",
                {
                    "payload": encoded,
                    "timestamp": 123,
                    "change_type": "order",
                    "op": "u",
                },
                "private-user",
            )
        ),
        {"market_selections"},
        {},
        order_channels={"private-user"},
    )

    assert handled is True
    assert callback_updates == [(order, 123, 12.5, 0)]
    assert stream.cache.snapshots() == {}


def test_establish_routes_registered_private_order_updates() -> None:
    channels = [_private_user_channel(), _channel(1)]
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authenticated": {"auth": "signin", "user_data": "{}"},
        "authorized_channel": channels,
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": [],
        "subscriptions": [],
    }
    order = {
        "info": {
            "order_id": "order-1",
            "external_id": "external-1",
            "filled_quantity": 1,
            "open_quantity": 0,
            "matching_status": "fully_matched",
            "status": "open",
        }
    }
    encoded = base64.b64encode(orjson.dumps(order)).decode()
    socket = FakeSocket(
        [
            _frame(
                "pusher:connection_established",
                orjson.dumps({"socket_id": "1.2"}).decode(),
            ),
            _frame("pusher:signin_success", "{}"),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[0]["channel_name"],
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[1]["channel_name"],
            ),
            _frame(
                "order",
                {
                    "payload": encoded,
                    "timestamp": 123,
                    "change_type": "order",
                    "op": "u",
                },
                channels[0]["channel_name"],
            ),
        ]
    )
    updates: list[dict[str, Any]] = []
    stream = ProphetXWebSocket(
        FakeClient(registration),
        on_order_update=lambda payload, *_args: updates.append(payload),
    )
    socket.on_last_recv = stream._stop.set

    stream._establish(socket, (1,), (), 0)

    assert updates == [order]


@pytest.mark.parametrize(
    ("event_name", "operation", "channel", "scope_map"),
    [
        ("sport_event", "d", "private-tournaments", {}),
        ("market", "u", "private-event=1-all", {"private-event=1-all": (1, None)}),
        (
            "market_selections",
            "d",
            "private-event=1-all",
            {"private-event=1-all": (1, None)},
        ),
    ],
)
def test_structural_and_delete_events_invalidate_books(
    event_name: str,
    operation: str,
    channel: str,
    scope_map: dict[str, tuple[int, str | None]],
) -> None:
    cache = MarketCache()
    request = cache.begin_reconcile((1,))
    cache.apply_authoritative_snapshot(
        1,
        {
            "markets": [
                {
                    "id": 10,
                    "selections": [
                        [
                            {
                                "strike_id": "s",
                                "outcome_id": 8,
                                "price": 120,
                                "quantity": 3,
                            }
                        ]
                    ],
                }
            ]
        },
        request=request,
    )
    assert cache.event_snapshot(1).valid is True

    payload = base64.b64encode(orjson.dumps({"event_id": 1})).decode()
    stream = ProphetXWebSocket(FakeClient({}), cache=cache)

    changed = stream._handle_update(
        _decode(
            _frame(
                event_name,
                {
                    "payload": payload,
                    "change_type": event_name,
                    "op": operation,
                },
                channel,
            )
        ),
        {event_name},
        scope_map,
    )

    assert changed is True
    assert cache.event_snapshot(1).valid is False


def test_subtype_only_disconnect_requires_refresh_and_old_teardown_is_harmless() -> (
    None
):
    cache = MarketCache()
    subtype = "touch_down"
    channel = _channel(42, subtype)
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": [channel],
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }
    stream = ProphetXWebSocket(
        FakeClient(registration),
        cache=cache,
        reconnect_base=0,
        reconnect_cap=0,
    )
    stream.set_subscriptions((), ((42, subtype),))
    request = cache.begin_reconcile((42,))
    cache.apply_authoritative_snapshot(
        42,
        {
            "markets": [
                {
                    "id": 10,
                    "selections": [
                        [
                            {
                                "strike_id": "s",
                                "outcome_id": 8,
                                "price": 120,
                                "quantity": 3,
                            }
                        ]
                    ],
                }
            ]
        },
        request=request,
    )

    class DisconnectSocket(FakeSocket):
        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            raise OSError("disconnect")

    class ReconnectSocket(FakeSocket):
        def __init__(self) -> None:
            super().__init__(
                [
                    _frame(
                        "pusher:connection_established",
                        orjson.dumps({"socket_id": "3.4"}).decode(),
                    ),
                    _frame(
                        "pusher_internal:subscription_succeeded",
                        {},
                        channel["channel_name"],
                    ),
                ]
            )
            self.blocked = threading.Event()
            self.release = threading.Event()

        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            self.blocked.set()
            assert self.release.wait(5)
            raise OSError("obsolete teardown")

    first = DisconnectSocket(
        [
            _frame(
                "pusher:connection_established",
                orjson.dumps({"socket_id": "1.2"}).decode(),
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channel["channel_name"],
            ),
        ]
    )
    second = ReconnectSocket()
    sockets = [first, second]

    def connector(_url: str, **_kwargs: Any) -> FakeSocket:
        if sockets:
            return sockets.pop(0)
        with stream._lock:
            assert stream._worker_cancel is not None
            stream._worker_cancel.set()
        raise OSError("done")

    stream._connector = connector
    stream.start((), ((42, subtype),))
    worker = stream._thread
    assert worker is not None
    assert second.blocked.wait(5)

    assert stream.ready is True
    assert stream.books_ready((42,)) is False
    assert cache.current_levels((42, "10", "s", "8")) == ()

    stream.set_subscriptions((42,), ((42, subtype),))
    request = cache.begin_reconcile((42,))
    cache.apply_authoritative_snapshot(
        42,
        {
            "markets": [
                {
                    "id": 10,
                    "selections": [
                        [
                            {
                                "strike_id": "s",
                                "outcome_id": 8,
                                "price": 130,
                                "quantity": 4,
                            }
                        ]
                    ],
                }
            ]
        },
        request=request,
    )
    second.release.set()
    worker.join(5)

    assert not worker.is_alive()
    assert cache.books_valid((42,)) is True
    assert cache.current_levels((42, "10", "s", "8"))[0].price == 130


@pytest.mark.parametrize("signin", [False, True])
def test_handshake_read_timeout_can_recover_before_deadline(signin: bool) -> None:
    channel = _channel(1)
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": [channel],
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }
    frames: list[str | BaseException] = [
        _frame(
            "pusher:connection_established",
            orjson.dumps({"socket_id": "1.2"}).decode(),
        )
    ]
    if signin:
        registration["authenticated"] = {"auth": "signin", "user_data": "{}"}
        frames.extend(
            [
                TimeoutError(),
                _frame("pusher:signin_success", "{}"),
            ]
        )
    frames.extend(
        [
            TimeoutError(),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channel["channel_name"],
            ),
        ]
    )
    socket = FakeSocket(frames)
    stream = ProphetXWebSocket(FakeClient(registration))
    generation = stream.set_subscriptions((1,), ())
    socket.on_last_recv = stream._stop.set

    stream._establish(socket, (1,), (), generation)

    assert stream.ready is True


@pytest.mark.parametrize("phase", ["ack", "steady"])
def test_superseded_buffered_frame_never_mutates_cache_or_callback(
    phase: str,
) -> None:
    channel = _channel(1)
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": [channel],
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }
    old_payload = base64.b64encode(
        orjson.dumps(
            {
                "market_selections": [
                    {
                        "market_id": 10,
                        "strike_id": "s",
                        "outcome_id": 8,
                        "price": 120,
                        "quantity": 1,
                    }
                ]
            }
        )
    ).decode()
    buffered_frame = _frame(
        "market_selections",
        {"payload": old_payload},
        channel["channel_name"],
    )

    class BlockingSocket(FakeSocket):
        def __init__(self) -> None:
            frames = [
                _frame(
                    "pusher:connection_established",
                    orjson.dumps({"socket_id": "1.2"}).decode(),
                )
            ]
            if phase == "steady":
                frames.append(
                    _frame(
                        "pusher_internal:subscription_succeeded",
                        {},
                        channel["channel_name"],
                    )
                )
            super().__init__(frames)
            self.blocked = threading.Event()
            self.release = threading.Event()

        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            self.blocked.set()
            assert self.release.wait(5)
            return buffered_frame

    callbacks: list[int] = []

    def on_update(
        _event_id: int,
        _subtype: str | None,
        _payload: dict[str, Any],
        _exchange_timestamp: int | float | None,
        _received: float,
        generation: int,
    ) -> None:
        callbacks.append(generation)

    cache = MarketCache()
    stream = ProphetXWebSocket(
        FakeClient(registration),
        cache=cache,
        on_update=on_update,
    )
    old_generation = stream.set_subscriptions((1,), ())
    socket = BlockingSocket()
    worker = threading.Thread(
        target=stream._establish,
        args=(socket, (1,), (), old_generation),
    )
    worker.start()
    assert socket.blocked.wait(5)

    new_generation = stream.set_subscriptions((1, 2), ())
    assert new_generation != old_generation
    request = cache.begin_reconcile((1,))
    cache.apply_authoritative_snapshot(
        1,
        {
            "markets": [
                {
                    "id": 10,
                    "selections": [
                        [
                            {
                                "strike_id": "s",
                                "outcome_id": 8,
                                "price": 120,
                                "quantity": 9,
                            }
                        ]
                    ],
                }
            ]
        },
        request=request,
    )
    socket.release.set()
    worker.join(5)

    assert not worker.is_alive()
    assert cache.current_levels((1, "10", "s", "8"))[0].quantity == 9
    assert callbacks == []


def test_obsolete_invalidation_cannot_clear_replacement_reconcile() -> None:
    cache = MarketCache()
    stream = ProphetXWebSocket(FakeClient({}), cache=cache)
    old_generation = stream.set_subscriptions((42,), ())
    old_request = cache.begin_reconcile((42,))
    cache.apply_authoritative_snapshot(
        42,
        {"markets": []},
        request=old_request,
    )
    waiting = threading.Event()
    release = threading.Event()

    def obsolete_teardown() -> None:
        waiting.set()
        assert release.wait(5)
        stream._invalidate_generation_books(
            old_generation,
            (42,),
            (),
        )

    worker = threading.Thread(target=obsolete_teardown, name="obsolete")
    worker.start()
    assert waiting.wait(5)
    replacement_generation = stream.set_subscriptions(
        (),
        ((42, "touch_down"),),
    )
    replacement_request = cache.begin_reconcile((42,))
    cache.apply_authoritative_snapshot(
        42,
        {
            "markets": [
                {
                    "id": 20,
                    "selections": [
                        [
                            {
                                "strike_id": "s",
                                "outcome_id": 1,
                                "price": 105,
                                "quantity": 2,
                            }
                        ]
                    ],
                }
            ]
        },
        request=replacement_request,
    )
    release.set()
    worker.join(5)
    assert replacement_generation != old_generation
    assert cache.books_valid((42,)) is True
    assert cache.current_levels((42, "20", "s", "1"))[0].price == 105


@pytest.mark.parametrize("phase", ["ack", "update"])
def test_close_fence_rejects_buffered_frames(phase: str) -> None:
    channel = _channel(1)
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": [channel],
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }
    update_payload = base64.b64encode(
        orjson.dumps(
            {
                "market_selections": [
                    {
                        "market_id": 10,
                        "strike_id": "s",
                        "outcome_id": 1,
                        "price": 120,
                        "quantity": 1,
                    }
                ]
            }
        )
    ).decode()
    buffered = (
        _frame(
            "pusher_internal:subscription_succeeded",
            {},
            channel["channel_name"],
        )
        if phase == "ack"
        else _frame(
            "market_selections",
            {"payload": update_payload},
            channel["channel_name"],
        )
    )

    class BlockingSocket(FakeSocket):
        def __init__(self) -> None:
            frames = [
                _frame(
                    "pusher:connection_established",
                    orjson.dumps({"socket_id": "1.2"}).decode(),
                )
            ]
            if phase == "update":
                frames.append(
                    _frame(
                        "pusher_internal:subscription_succeeded",
                        {},
                        channel["channel_name"],
                    )
                )
            super().__init__(frames)
            self.blocked = threading.Event()
            self.release = threading.Event()

        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            self.blocked.set()
            assert self.release.wait(5)
            return buffered

    callbacks: list[int] = []
    stream = ProphetXWebSocket(
        FakeClient(registration),
        on_update=lambda *_args: callbacks.append(1),
    )
    generation = stream.set_subscriptions((1,), ())
    socket = BlockingSocket()
    worker = threading.Thread(
        target=stream._establish,
        args=(socket, (1,), (), generation),
    )
    worker.start()
    assert socket.blocked.wait(5)

    stream.close()
    socket.release.set()
    worker.join(5)

    assert not worker.is_alive()
    assert stream.ready is False
    assert stream.cache.authorized_scopes == frozenset()
    assert stream.cache.current_levels((1, "10", "s", "1")) == ()
    assert callbacks == []


def test_callback_restart_during_shutdown_replaces_worker_without_deadlock() -> None:
    class LifecycleClient(FakeClient):
        def __init__(self) -> None:
            super().__init__({})

        def register_websocket(
            self,
            socket_id: str,
            event_ids: tuple[int, ...],
            pairs: tuple[tuple[int, str], ...],
        ) -> dict[str, Any]:
            del socket_id, pairs
            channels = [_channel(event_id) for event_id in event_ids]
            return {
                "success": True,
                "status": "CONNECTED",
                "authorized_channel": channels,
                "channel_count": len(event_ids),
                "channel_limit": 100,
                "rejected": None,
                "subscriptions": [],
            }

    class WorkerSocket(FakeSocket):
        def __init__(
            self,
            event_id: int,
            socket_id: str,
            update: str | None = None,
        ) -> None:
            channel = _channel(event_id)
            frames = [
                _frame(
                    "pusher:connection_established",
                    orjson.dumps({"socket_id": socket_id}).decode(),
                ),
                _frame(
                    "pusher_internal:subscription_succeeded",
                    {},
                    channel["channel_name"],
                ),
            ]
            if update is not None:
                frames.append(update)
            super().__init__(frames)
            self.blocked = threading.Event()
            self.release = threading.Event()
            self.closed = threading.Event()

        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            self.blocked.set()
            assert self.release.wait(5)
            raise OSError("worker exit")

        def close(self) -> None:
            self.closed.set()

    update_payload = base64.b64encode(
        orjson.dumps(
            {
                "market_selections": [
                    {
                        "market_id": 10,
                        "strike_id": "s",
                        "outcome_id": 1,
                        "price": 120,
                        "quantity": 1,
                    }
                ]
            }
        )
    ).decode()
    first = WorkerSocket(
        1,
        "1.2",
        _frame(
            "market_selections",
            {"payload": update_payload},
            _channel(1)["channel_name"],
        ),
    )
    second = WorkerSocket(2, "3.4")
    sockets = [first, second]

    def connector(_url: str, **_kwargs: Any) -> WorkerSocket:
        return sockets.pop(0)

    callback_entered = threading.Event()
    release_callback = threading.Event()
    restarted: list[int] = []
    stream: ProphetXWebSocket

    def on_update(*_args: Any) -> None:
        callback_entered.set()
        assert release_callback.wait(5)
        restarted.append(stream.start((2,), ()))

    stream = ProphetXWebSocket(
        LifecycleClient(),
        connector=connector,
        on_update=on_update,
        reconnect_base=0,
        reconnect_cap=0,
    )
    stream.start((1,), ())
    assert callback_entered.wait(5)
    old_worker = stream._thread
    closer = threading.Thread(target=stream.close)
    closer.start()
    assert first.closed.wait(5)

    release_callback.set()
    closer.join(5)
    assert second.blocked.wait(5)

    assert not closer.is_alive()
    assert restarted == [stream.generation]
    assert stream.event_ids == (2,)
    assert stream._thread is not old_worker
    assert stream._thread is not None and stream._thread.is_alive()
    assert stream._socket is second
    assert stream._socket_owner_token == stream._worker_token
    assert stream.ready is True

    second.release.set()
    stream.close()


def test_superseded_final_ack_cannot_publish_readiness() -> None:
    channel = _channel(1)
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": [channel],
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }
    socket = FakeSocket(
        [
            _frame(
                "pusher:connection_established",
                orjson.dumps({"socket_id": "1.2"}).decode(),
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channel["channel_name"],
            ),
        ]
    )
    ack_checked = threading.Event()
    release_ack = threading.Event()
    stream = ProphetXWebSocket(FakeClient(registration))
    generation = stream.set_subscriptions((1,), ())
    original_handle_control = stream._handle_control

    def blocking_handle_control(ws: Any, frame: dict[str, Any]) -> bool:
        if frame.get("event") == "pusher_internal:subscription_succeeded":
            ack_checked.set()
            assert release_ack.wait(5)
        return original_handle_control(ws, frame)

    stream._handle_control = blocking_handle_control
    worker = threading.Thread(
        target=stream._establish,
        args=(socket, (1,), (), generation),
    )
    worker.start()
    assert ack_checked.wait(5)

    stream.set_subscriptions((1, 2), ())
    release_ack.set()
    worker.join(5)

    assert not worker.is_alive()
    assert stream.wait_ready(0) is False
    assert stream.cache.ready is False
    assert stream.cache.authorized_scopes == frozenset()


@pytest.mark.parametrize(
    ("scope_map", "expected_event_id"),
    [
        ({}, 44),
        ({"private-event=44-subtype=winner": (45, None)}, 45),
    ],
)
def test_channel_scope_map_precedes_defensive_name_fallback(
    scope_map: dict[str, tuple[int, str | None]],
    expected_event_id: int,
) -> None:
    cache = MarketCache()
    stream = ProphetXWebSocket(FakeClient({}), cache=cache)
    payload = base64.b64encode(
        orjson.dumps(
            {
                "market_selections": [
                    {
                        "market_id": 1,
                        "strike_id": "s",
                        "name": "away",
                        "price": -120,
                        "quantity": 2,
                    }
                ]
            }
        )
    ).decode()
    changed = stream._handle_update(
        _decode(
            _frame(
                "market_selections",
                {"payload": payload},
                "private-event=44-subtype=winner",
            )
        ),
        {"market_selections"},
        scope_map,
    )
    assert changed is True
    assert cache.snapshot((expected_event_id, "1", "s", "away")) is not None


def test_pusher_pings_do_not_mask_stale_private_health() -> None:
    clock = [0.0]
    channels = [_global_channel("private-user"), _channel(1)]
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": channels,
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }

    class PingSocket(FakeSocket):
        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            clock[0] += 0.6
            return _frame("pusher:ping", {})

    socket = PingSocket(
        [
            _frame(
                "pusher:connection_established",
                orjson.dumps({"socket_id": "1.2"}).decode(),
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[0]["channel_name"],
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[1]["channel_name"],
            ),
        ]
    )
    stream = ProphetXWebSocket(
        FakeClient(registration),
        health_timeout=1.0,
        monotonic=lambda: clock[0],
    )

    with pytest.raises(TimeoutError, match="private health_check"):
        stream._establish(socket, (1,), (), 0)

    assert [
        frame["event"] for frame in socket.sent if frame["event"] == "pusher:pong"
    ] == ["pusher:pong", "pusher:pong"]


def test_pusher_pings_remain_health_when_no_private_binding_exists() -> None:
    clock = [0.0]
    channels = [_channel(1)]
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": channels,
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }

    class PingSocket(FakeSocket):
        ping_count = 0

        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            self.ping_count += 1
            clock[0] += 0.6
            if self.ping_count == 3:
                stream._stop.set()
            return _frame("pusher:ping", {})

    socket = PingSocket(
        [
            _frame(
                "pusher:connection_established",
                orjson.dumps({"socket_id": "1.2"}).decode(),
            ),
            _frame(
                "pusher_internal:subscription_succeeded",
                {},
                channels[0]["channel_name"],
            ),
        ]
    )
    stream = ProphetXWebSocket(
        FakeClient(registration),
        health_timeout=1.0,
        monotonic=lambda: clock[0],
    )

    stream._establish(socket, (1,), (), 0)

    assert socket.ping_count == 3


def test_reconnect_backoff_caps_and_resets_after_ready_session() -> None:
    class Stop(threading.Event):
        def __init__(self) -> None:
            super().__init__()
            self.waits: list[float] = []

        def wait(self, timeout: float | None = None) -> bool:
            self.waits.append(0.0 if timeout is None else timeout)
            return False

    channel = _channel(1)
    registration = {
        "success": True,
        "status": "CONNECTED",
        "authorized_channel": [channel],
        "channel_count": 1,
        "channel_limit": 100,
        "rejected": None,
        "subscriptions": [],
    }
    stream = ProphetXWebSocket(
        FakeClient(registration),
        reconnect_base=0.25,
        reconnect_cap=8.0,
        random_source=lambda: 1.0,
    )
    stream.set_subscriptions((1,), ())
    stop = Stop()
    stream._worker_token = 1
    stream._worker_cancel = stop
    calls = 0

    class ReadyThenDisconnect(FakeSocket):
        def recv(self, timeout: float | None = None) -> str:
            if self.frames:
                return super().recv(timeout)
            raise OSError("post-ready disconnect")

    def connector(_url: str, **_kwargs: Any) -> FakeSocket:
        nonlocal calls
        calls += 1
        if calls <= 1100:
            raise OSError("offline")
        if calls == 1101:
            return ReadyThenDisconnect(
                [
                    _frame(
                        "pusher:connection_established",
                        orjson.dumps({"socket_id": "1.2"}).decode(),
                    ),
                    _frame(
                        "pusher_internal:subscription_succeeded",
                        {},
                        channel["channel_name"],
                    ),
                ]
            )
        stop.set()
        raise OSError("done")

    stream._connector = connector
    stream._run(1, stop)

    assert calls == 1102
    assert max(stop.waits) <= 8.0
    assert stop.waits[-1] == 0.25


def _decode(value: str) -> dict[str, Any]:
    return orjson.loads(value)
