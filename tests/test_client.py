from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest

from fastprophetx import (
    AuthenticationError,
    OrderIntent,
    ProphetXAPIError,
    RateLimitPolicy,
    RequestScheduler,
    ResponseError,
    TransportError,
)


def _login(token: str = "access-1") -> dict[str, Any]:
    return {
        "data": {
            "access_token": token,
            "refresh_token": "refresh-1",
            "access_expire_time": 61,
        }
    }


def test_explicit_environment_and_from_env() -> None:
    from fastprophetx import Environment, ProphetXClient

    with pytest.raises(ValueError):
        ProphetXClient("a", "b", environment="staging")
    client = ProphetXClient.from_env(
        "production",
        environ={
            "PROPHETX_ACCESS_KEY": "a",
            "PROPHETX_SECRET_KEY": "b",
        },
    )
    assert client.environment is Environment.PRODUCTION
    client.close()


def test_auth_header_and_thread_safe_refresh(make_client: Any) -> None:
    now = [100.0]
    refreshes = 0
    authorization: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal refreshes
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/auth/refresh"):
            refreshes += 1
            return httpx.Response(
                200,
                json={"data": {"access_token": "access-2", "access_expire_time": 61}},
                request=request,
            )
        authorization.append(request.headers["Authorization"])
        return httpx.Response(200, json={"data": {"balance": 1}}, request=request)

    client = make_client(
        handler,
        clock=lambda: now[0],
        wall_clock=lambda: 1_700_000_000.0,
    )
    client.authenticate()
    now[0] += 2
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: client.get_balance(), range(2)))
    assert results == [{"balance": 1}, {"balance": 1}]
    assert refreshes == 1
    assert authorization == ["Bearer access-2", "Bearer access-2"]


def test_balance_requests_use_spaced_endpoint_limit(make_client: Any) -> None:
    now = [0.0]
    sleeps: list[float] = []

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        now[0] += delay

    scheduler = RequestScheduler(
        RateLimitPolicy(
            requests_per_second=1_000_000,
            market_path_spacing=1.05,
        ),
        clock=lambda: now[0],
        sleep=sleep,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        return httpx.Response(200, json={"data": {"balance": 1}}, request=request)

    client = make_client(
        handler,
        scheduler=scheduler,
        clock=lambda: now[0],
        sleep=sleep,
    )
    client.get_balance()
    client.get_balance()

    assert sum(sleeps) >= 1.05


def test_definitive_401_refreshes_once(make_client: Any) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/auth/refresh"):
            return httpx.Response(
                200,
                json={"data": {"access_token": "access-2", "access_expire_time": 600}},
                request=request,
            )
        calls += 1
        if calls == 1:
            return httpx.Response(401, json={"error": "expired"}, request=request)
        return httpx.Response(200, json={"data": {"balance": 5}}, request=request)

    client = make_client(handler)
    assert client.get_balance()["balance"] == 5
    assert calls == 2


def test_expired_refresh_token_falls_back_to_login(make_client: Any) -> None:
    logins = 0
    refreshes = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal logins, refreshes
        if request.url.path.endswith("/auth/login"):
            logins += 1
            return httpx.Response(
                200,
                json=_login(f"access-{logins}"),
                request=request,
            )
        if request.url.path.endswith("/auth/refresh"):
            refreshes += 1
            return httpx.Response(401, json={"error": "expired"}, request=request)
        if request.headers["Authorization"] != "Bearer access-2":
            return httpx.Response(401, json={"error": "expired"}, request=request)
        return httpx.Response(200, json={"data": {"balance": 5}}, request=request)

    client = make_client(handler)
    client.authenticate()
    assert client.get_balance()["balance"] == 5
    assert logins == 2
    assert refreshes == 1


def test_429_read_retry_and_cooldown(make_client: Any) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                json={"details": {"next_allow_at": 1_700_000_000_000}},
                headers={"Retry-After": "0"},
                request=request,
            )
        return httpx.Response(200, json={"data": {"tournaments": []}}, request=request)

    client = make_client(handler)
    assert client.get_tournaments() == []
    assert calls == 2
    assert client.scheduler.diagnostics()["cooldowns"] == 1


def test_multiple_markets_normalizes_live_null_event_to_empty(
    make_client: Any,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        return httpx.Response(200, json={"data": {"1": None}}, request=request)

    assert make_client(handler).get_multiple_markets([1]) == {"1": []}


def test_mutation_429_cools_down_next_independent_request(make_client: Any) -> None:
    now = [0.0]
    sleeps: list[float] = []
    submissions = 0

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        now[0] += delay

    scheduler = RequestScheduler(
        RateLimitPolicy(
            requests_per_second=1_000_000,
            market_path_spacing=0,
            max_429_retries=0,
        ),
        clock=lambda: now[0],
        sleep=sleep,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal submissions
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/get_price_ladder"):
            return httpx.Response(200, json={"data": [-110]}, request=request)
        if request.url.path.endswith("/submit_multiple_orders"):
            submissions += 1
            return httpx.Response(
                429,
                json={"error": "rate_limit_reached"},
                headers={"Retry-After": "2"},
                request=request,
            )
        return httpx.Response(200, json={"data": {"balance": 1}}, request=request)

    client = make_client(
        handler,
        scheduler=scheduler,
        clock=lambda: now[0],
        sleep=sleep,
    )
    with pytest.raises(ProphetXAPIError) as caught:
        client.submit_multiple_orders([OrderIntent("x", "strike", -110, 1)])
    assert caught.value.outcome_unknown is False
    assert submissions == 1

    assert client.get_balance() == {"balance": 1}
    assert sum(sleeps) >= 2.0


def test_mutation_transport_is_not_retried_and_unknown(make_client: Any) -> None:
    submissions = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal submissions
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/get_price_ladder"):
            return httpx.Response(200, json={"data": [-110]}, request=request)
        if request.url.path.endswith("/submit_multiple_orders"):
            submissions += 1
            raise httpx.ConnectError("connection lost", request=request)
        raise AssertionError(request.url)

    client = make_client(handler)
    with pytest.raises(TransportError) as caught:
        client.submit_multiple_orders([OrderIntent("x", "strike", -110, 1)])
    assert caught.value.outcome_unknown is True
    assert submissions == 1


def test_duplicate_external_ids_are_rejected_before_network(
    make_client: Any,
) -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise AssertionError(request.url)

    client = make_client(handler)
    with pytest.raises(ValueError, match="external_id values must be unique"):
        client.submit_multiple_orders(
            [
                OrderIntent("duplicate", "strike-1", -110, 1),
                OrderIntent("duplicate", "strike-2", 120, 1),
            ]
        )
    assert requests == 0


def test_normal_order_omits_strategy_and_explicit_strategy_is_preserved(
    make_client: Any,
) -> None:
    submitted: list[list[dict[str, Any]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/get_price_ladder"):
            return httpx.Response(200, json={"data": [-110]}, request=request)
        body = json.loads(request.content)
        submitted.append(body["data"])
        return httpx.Response(
            200,
            json={
                "data": {
                    "succeed_orders": [
                        {
                            "external_id": order["external_id"],
                            "order_id": f"id-{index}",
                        }
                        for index, order in enumerate(body["data"])
                    ],
                    "failed_orders": [],
                }
            },
            request=request,
        )

    client = make_client(handler)
    client.submit_multiple_orders(
        [
            OrderIntent("normal", "strike", -110, 1),
            OrderIntent("post-only", "strike", -110, 1, "ALO"),
        ]
    )

    assert "order_strategy" not in submitted[0][0]
    assert submitted[0][1]["order_strategy"] == "ALO"


def test_submit_order_returns_typed_result(make_client: Any) -> None:
    submitted: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/get_price_ladder"):
            return httpx.Response(200, json={"data": [-110]}, request=request)
        submitted.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "data": {
                    "success": True,
                    "order": {
                        "external_id": "single",
                        "order_id": "order-1",
                        "status": "open",
                    },
                }
            },
            request=request,
        )

    result = make_client(handler).submit_order(OrderIntent("single", "strike", -110, 2))

    assert "order_strategy" not in submitted
    assert result.external_id == "single"
    assert result.order_id == "order-1"
    assert result.status == "open"


def test_get_strikes_uses_documented_batch_shape(make_client: Any) -> None:
    seen_query = ""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_query
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        seen_query = request.url.params["strike_ids"]
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "event_id": 1,
                        "market_id": "market",
                        "strike_id": "strike-a",
                        "type": "moneyline",
                        "sub_type": None,
                    }
                ]
            },
            request=request,
        )

    strikes = make_client(handler).get_strikes(["strike-a", "strike-a"])

    assert seen_query == "strike-a"
    assert strikes[0]["strike_id"] == "strike-a"


def test_focused_v4_history_endpoints_keep_cursor_and_sync_metadata(
    make_client: Any,
) -> None:
    queries: dict[str, dict[str, str]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        name = request.url.path.rsplit("/", 1)[-1]
        queries[name] = dict(request.url.params)
        if name == "get_order_matched_detail":
            data = {"matching_details": [{"order_id": "order-1"}], "next_cursor": "a"}
        elif name == "get_trades":
            data = {"trades": [{"id": "trade-1"}], "next_cursor": "b"}
        else:
            data = {"transactions": [{"trade_id": "trade-1"}], "next_cursor": "c"}
        return httpx.Response(
            200,
            json={"data": data, "last_synced_at": 123},
            request=request,
        )

    client = make_client(handler)
    matched = client.get_order_matched_detail_page(
        order_ids=["order-1", "order-2"],
        from_timestamp=10,
        to_timestamp=20,
    )
    trades = client.get_trades_page(from_timestamp=10, to_timestamp=20)
    transactions = client.get_transactions_page(
        transaction_type="TRADE",
        trade_id="trade-1",
    )

    assert queries["get_order_matched_detail"]["order_ids"] == "order-1,order-2"
    assert matched["last_synced_at"] == 123
    assert trades["next_cursor"] == "b"
    assert queries["get_transactions"]["transaction_type"] == "TRADE"
    assert transactions["transactions"][0]["trade_id"] == "trade-1"


def test_bulk_and_scoped_cancellations_use_documented_bodies(
    make_client: Any,
) -> None:
    requests: list[tuple[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        body = json.loads(request.content) if request.content else None
        requests.append((request.url.path.rsplit("/", 1)[-1], body))
        if request.url.path.endswith("/cancel_multiple_orders"):
            return httpx.Response(
                200,
                json={"data": [{"success": True, "order": {"order_id": "order-1"}}]},
                request=request,
            )
        return httpx.Response(
            200,
            json={"data": {"success": True}},
            request=request,
        )

    client = make_client(handler)
    client.cancel_multiple_orders([("order-1", "external-1")])
    client.cancel_all_orders()
    client.cancel_orders_by_event(12)
    client.cancel_orders_by_market(12, "market-1", "strike-1")

    assert requests == [
        (
            "cancel_multiple_orders",
            {"data": [{"order_id": "order-1", "external_id": "external-1"}]},
        ),
        ("cancel_all_orders", None),
        ("cancel_orders_by_event", {"event_id": 12}),
        (
            "cancel_orders_by_market",
            {
                "event_id": 12,
                "market_id": "market-1",
                "strike_id": "strike-1",
            },
        ),
    ]


@pytest.mark.parametrize(
    "data",
    [
        {"succeed_orders": [{"external_id": "a"}], "failed_orders": []},
        {
            "succeed_orders": [
                {"external_id": "a", "order_id": "1"},
                {"external_id": "a", "order_id": "2"},
            ],
            "failed_orders": [],
        },
        {
            "succeed_orders": [{"external_id": "a", "order_id": "1"}],
            "failed_orders": [
                {
                    "index": 0,
                    "request": {"external_id": "a"},
                    "error": "rejected",
                }
            ],
        },
        {
            "succeed_orders": [{"external_id": "unknown", "order_id": "1"}],
            "failed_orders": [],
        },
        {
            "succeed_orders": [],
            "failed_orders": [
                {
                    "index": 0,
                    "request": {"external_id": "b"},
                    "error": "rejected",
                }
            ],
        },
    ],
)
def test_conflicting_batch_responses_are_outcome_unknown(
    make_client: Any,
    data: dict[str, Any],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/get_price_ladder"):
            return httpx.Response(200, json={"data": [-110, 120]}, request=request)
        return httpx.Response(200, json={"data": data}, request=request)

    client = make_client(handler)
    with pytest.raises(ResponseError) as caught:
        client.submit_multiple_orders(
            [
                OrderIntent("a", "strike-a", -110, 1),
                OrderIntent("b", "strike-b", 120, 1),
            ]
        )
    assert caught.value.outcome_unknown is True


@pytest.mark.parametrize("status", [408, 500, 503])
def test_mutation_server_failure_is_unknown(make_client: Any, status: int) -> None:
    submissions = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal submissions
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/get_price_ladder"):
            return httpx.Response(200, json={"data": [-110]}, request=request)
        submissions += 1
        return httpx.Response(status, json={"error": "server"}, request=request)

    client = make_client(handler)
    with pytest.raises(ProphetXAPIError) as caught:
        client.submit_multiple_orders([OrderIntent("x", "strike", -110, 1)])
    assert caught.value.outcome_unknown is True
    assert submissions == 1


def test_malformed_mutation_success_is_unknown(make_client: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(200, json=_login(), request=request)
        if request.url.path.endswith("/get_price_ladder"):
            return httpx.Response(200, json={"data": [-110]}, request=request)
        return httpx.Response(200, content=b"not-json", request=request)

    client = make_client(handler)
    with pytest.raises(ResponseError) as caught:
        client.submit_multiple_orders([OrderIntent("x", "strike", -110, 1)])
    assert caught.value.outcome_unknown is True


def test_error_payload_is_structured_and_redacted(make_client: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(
                401,
                json={
                    "error": "bad_auth",
                    "message": {"access_token": "must-not-leak"},
                },
                request=request,
            )
        raise AssertionError

    client = make_client(handler)
    with pytest.raises(AuthenticationError) as caught:
        client.authenticate()
    assert caught.value.status == 401
    assert caught.value.code == "bad_auth"
    assert caught.value.details == {"access_token": "<redacted>"}
