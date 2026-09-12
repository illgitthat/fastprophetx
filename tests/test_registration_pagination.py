from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from fastprophetx import ResponseError


def _login(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "data": {
                "access_token": "access",
                "refresh_token": "refresh",
                "access_expire_time": 600,
            }
        },
        request=request,
    )


def _channel(event_id: int, subtype: str | None = None) -> dict[str, Any]:
    scope: dict[str, Any] = {"event_id": event_id}
    if subtype:
        scope["sub_type"] = subtype
    return {
        "channel_name": f"private-event={event_id}-{subtype or 'all'}",
        "auth": "channel-auth",
        "binding_events": [{"name": "market_selections"}],
        "scope": scope,
    }


def _global_channel(name: str) -> dict[str, Any]:
    return {
        "channel_name": name,
        "auth": "global-auth",
        "binding_events": [{"name": "health_check"}],
    }


def test_registration_uses_exact_pairs_and_current_shape(make_client: Any) -> None:
    body: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return _login(request)
        body.update(json.loads(request.content))
        subtype_channel = _channel(1, "Fóo!Bar / Baz-Qux")
        channels = [
            _global_channel("private-user"),
            _global_channel("private-tournaments"),
            _channel(1),
            _channel(2),
            subtype_channel,
        ]
        return httpx.Response(
            200,
            json={
                "data": {
                    "success": True,
                    "status": "CONNECTED",
                    "authenticated": {"auth": "signin", "user_data": "{}"},
                    "authorized_channel": channels,
                    "channel_count": 3,
                    "channel_limit": 100,
                    "rejected": [],
                    "subscriptions": body["subscriptions"],
                }
            },
            request=request,
        )

    client = make_client(handler)
    result = client.register_websocket(
        "1.2",
        [1, 2],
        [
            (1, "Fóo!Bar / Baz-Qux"),
            (1, "foobar_baz_qux"),
        ],
    )
    assert result["success"] is True
    assert "service" not in body
    assert body["subscriptions"] == [
        {"type": "event", "ids": ["1", "2"]},
        {
            "type": "event_subtype",
            "ids": ["1:foobar_baz_qux"],
        },
    ]


@pytest.mark.parametrize(
    "change",
    [
        {"success": False},
        {"status": "failed"},
        {"rejected": [{"type": "event", "id": "1", "reason": "no"}]},
        {"channel_count": 2},
    ],
)
def test_registration_validation(make_client: Any, change: dict[str, Any]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return _login(request)
        data = {
            "success": True,
            "status": "CONNECTED",
            "authorized_channel": [
                _global_channel("private-user"),
                _global_channel("private-tournaments"),
                _channel(1),
            ],
            "channel_count": 1,
            "channel_limit": 100,
            "rejected": None,
            "subscriptions": [],
        }
        data.update(change)
        return httpx.Response(200, json={"data": data}, request=request)

    with pytest.raises(ResponseError):
        make_client(handler).register_websocket("1.2", [1])


def test_pagination_and_find_external_id(make_client: Any) -> None:
    cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return _login(request)
        cursor = request.url.params.get("next_cursor")
        cursors.append(cursor)
        if cursor is None:
            data = {"orders": [{"external_id": "a"}], "next_cursor": "next"}
        else:
            data = {"orders": [{"external_id": "wanted"}], "next_cursor": ""}
        return httpx.Response(200, json={"data": data}, request=request)

    client = make_client(handler)
    assert client.find_order_by_external_id(
        "wanted",
        from_timestamp=1,
    ) == {"external_id": "wanted"}
    assert cursors == [None, "next"]


def test_order_reads_preserve_last_synced_at(make_client: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return _login(request)
        if "/get_order/" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "data": {"order_id": "order-1", "status": "open"},
                    "last_synced_at": 123,
                },
                request=request,
            )
        return httpx.Response(
            200,
            json={
                "data": {"orders": [], "next_cursor": ""},
                "last_synced_at": 456,
            },
            request=request,
        )

    client = make_client(handler)

    assert client.get_order("order-1")["last_synced_at"] == 123
    assert client.get_order_history_page(from_timestamp=1)["last_synced_at"] == 456


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"from_timestamp": 2, "to_timestamp": 1}, "start cannot be after"),
        (
            {"from_timestamp": 1, "to_timestamp": 1 + 7 * 24 * 60 * 60 + 1},
            "cannot exceed 7 days",
        ),
        (
            {
                "updated_at_from": 1,
                "updated_at_to": 1 + 2 * 24 * 60 * 60 + 1,
            },
            "cannot exceed 2 days",
        ),
    ],
)
def test_order_history_rejects_invalid_documented_ranges(
    make_client: Any,
    kwargs: dict[str, int],
    message: str,
) -> None:
    client = make_client(lambda request: (_ for _ in ()).throw(AssertionError(request)))

    with pytest.raises(ValueError, match=message):
        client.get_order_history_page(**kwargs)


def test_cursor_cycle_detection(make_client: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return _login(request)
        return httpx.Response(
            200,
            json={"data": {"orders": [], "next_cursor": "same"}},
            request=request,
        )

    with pytest.raises(ResponseError, match="cursor repeated"):
        list(make_client(handler).iter_order_history(from_timestamp=1))


def test_find_order_requires_history_lower_bound(make_client: Any) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise AssertionError(request.url)

    client = make_client(handler)
    with pytest.raises(ValueError, match="at least one time boundary"):
        client.get_order_history_page()
    with pytest.raises(ValueError, match="from_timestamp or updated_at_from"):
        client.find_order_by_external_id("external")
    assert calls == 0
