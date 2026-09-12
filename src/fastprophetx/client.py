"""Dictionary-first ProphetX REST client."""

from __future__ import annotations

import email.utils
import os
import random
import threading
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

import httpx
import orjson

from .errors import (
    AuthenticationError,
    PreSubmitError,
    ProphetXAPIError,
    ResponseError,
    TransportError,
)
from .models import OrderIntent, OrderResult
from .odds import validate_american_odds
from .protocol import canonicalize_subtype
from .rate_limit import RateLimitPolicy, RequestScheduler

SANDBOX_BASE_URL = "https://api.sandbox.prophetx.dev/partner"
PRODUCTION_BASE_URL = "https://cash.api.prophetx.co/partner"
PROPHETX_SANDBOX_BASE_URL = SANDBOX_BASE_URL
PROPHETX_PRODUCTION_BASE_URL = PRODUCTION_BASE_URL


class Environment(StrEnum):
    SANDBOX = "sandbox"
    PRODUCTION = "production"


BASE_URLS = {
    Environment.SANDBOX: SANDBOX_BASE_URL,
    Environment.PRODUCTION: PRODUCTION_BASE_URL,
}
_ENV_VARS = {
    Environment.SANDBOX: (
        "PROPHETX_SANDBOX_ACCESS_KEY",
        "PROPHETX_SANDBOX_SECRET_KEY",
    ),
    Environment.PRODUCTION: ("PROPHETX_ACCESS_KEY", "PROPHETX_SECRET_KEY"),
}
_SPACED_QUERY_PATHS = frozenset(
    {
        "mm/get_sport_events",
        "mm/get_tournaments",
        "v4/mm/get_balance",
        "v4/mm/get_markets",
        "v4/mm/get_multiple_markets",
        "v4/mm/get_price_ladder",
    }
)
_DEFAULT_ACCESS_LIFETIME = 10 * 60.0
_REFRESH_MARGIN = 60.0
_MAX_ORDER_HISTORY_CREATED_RANGE = 7 * 24 * 60 * 60
_MAX_ORDER_HISTORY_UPDATED_RANGE = 2 * 24 * 60 * 60


class ProphetXClient:
    """Synchronous, reusable ProphetX partner API client."""

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        *,
        environment: str | Environment,
        timeout: float | httpx.Timeout = 5.0,
        transport: httpx.BaseTransport | None = None,
        scheduler: RequestScheduler | None = None,
        rate_limit_policy: RateLimitPolicy | None = None,
        clock: Any = time.monotonic,
        wall_clock: Any = time.time,
        sleep: Any = time.sleep,
        random_source: Any = random.random,
    ) -> None:
        environment = _normalize_environment(environment)
        if not isinstance(access_key, str) or not access_key.strip():
            raise ValueError("ProphetX access key cannot be empty")
        if not isinstance(secret_key, str) or not secret_key.strip():
            raise ValueError("ProphetX secret key cannot be empty")
        if scheduler is not None and rate_limit_policy is not None:
            raise ValueError("pass scheduler or rate_limit_policy, not both")

        self.environment = environment
        self.base_url = BASE_URLS[environment]
        self._access_key = access_key
        self._secret_key = secret_key
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._refresh_at = 0.0
        self._closed = False
        self._auth_lock = threading.RLock()
        self._price_ladder: frozenset[int] | None = None
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleep = sleep
        self._random = random_source
        self.scheduler = scheduler or RequestScheduler(
            rate_limit_policy, clock=clock, sleep=sleep
        )
        self._client = httpx.Client(
            base_url=f"{self.base_url}/",
            timeout=timeout,
            transport=transport,
            follow_redirects=False,
            http2=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=50,
                keepalive_expiry=30.0,
            ),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    @classmethod
    def from_credentials(
        cls,
        access_key: str,
        secret_key: str,
        *,
        environment: str | Environment,
        **kwargs: Any,
    ) -> Self:
        return cls(access_key, secret_key, environment=environment, **kwargs)

    @classmethod
    def from_env(
        cls,
        environment: str | Environment,
        *,
        environ: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> Self:
        selected = _normalize_environment(environment)
        values = os.environ if environ is None else environ
        access_name, secret_name = _ENV_VARS[selected]
        access_key = values.get(access_name, "")
        secret_key = values.get(secret_name, "")
        if not access_key:
            raise RuntimeError(f"missing required environment variable {access_name}")
        if not secret_key:
            raise RuntimeError(f"missing required environment variable {secret_name}")
        return cls(
            access_key,
            secret_key,
            environment=selected,
            **kwargs,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._client.close()

    def authenticate(self) -> dict[str, Any]:
        with self._auth_lock:
            payload = self._request(
                "POST",
                "auth/login",
                json={
                    "access_key": self._access_key,
                    "secret_key": self._secret_key,
                },
                authenticated=False,
                idempotent=True,
            )
            data = _require_dict_data(payload, "auth/login")
            access_token = data.get("access_token")
            refresh_token = data.get("refresh_token")
            if not isinstance(access_token, str) or not access_token:
                raise ResponseError("login response omitted access_token")
            if not isinstance(refresh_token, str) or not refresh_token:
                raise ResponseError("login response omitted refresh_token")
            self._access_token = access_token
            self._refresh_token = refresh_token
            self._set_refresh_deadline(data.get("access_expire_time"))
            return data

    def refresh_session(self) -> dict[str, Any]:
        with self._auth_lock:
            if not self._refresh_token:
                return self.authenticate()
            payload = self._request(
                "POST",
                "auth/refresh",
                json={"refresh_token": self._refresh_token},
                authenticated=False,
                idempotent=True,
            )
            data = _require_dict_data(payload, "auth/refresh")
            access_token = data.get("access_token")
            if not isinstance(access_token, str) or not access_token:
                raise ResponseError("refresh response omitted access_token")
            self._access_token = access_token
            self._set_refresh_deadline(data.get("access_expire_time"))
            return data

    def warmup(self, *, price_ladder: bool = True) -> dict[str, Any]:
        auth = self.authenticate()
        result: dict[str, Any] = {"authenticated": True, "auth": auth}
        if price_ladder:
            result["price_ladder"] = self.get_price_ladder()
        return result

    def get_balance(self) -> dict[str, Any]:
        return _require_dict_data(
            self._request("GET", "v4/mm/get_balance"), "v4/mm/get_balance"
        )

    def get_tournaments(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        data = _require_dict_data(
            self._request(
                "GET",
                "mm/get_tournaments",
                params={"has_active_events": str(active_only).lower()},
            ),
            "mm/get_tournaments",
        )
        return _require_dict_list(data, "tournaments", "mm/get_tournaments")

    def get_sport_events(
        self,
        tournament_id: int | None = None,
        *,
        event_ids: Iterable[int] = (),
    ) -> list[dict[str, Any]]:
        ids = _positive_unique_ids(event_ids, "event IDs")
        if tournament_id is None and not ids:
            raise ValueError("tournament_id or event_ids is required")
        if tournament_id is not None and tournament_id <= 0:
            raise ValueError("tournament_id must be positive")
        params: dict[str, Any] = {}
        if tournament_id is not None:
            params["tournament_id"] = tournament_id
        if ids:
            params["event_ids"] = ",".join(map(str, ids))
        data = _require_dict_data(
            self._request("GET", "mm/get_sport_events", params=params),
            "mm/get_sport_events",
        )
        return _require_dict_list(data, "sport_events", "mm/get_sport_events")

    def get_markets(self, event_id: int) -> list[dict[str, Any]]:
        if event_id <= 0:
            raise ValueError("event_id must be positive")
        data = _require_dict_data(
            self._request("GET", "v4/mm/get_markets", params={"event_id": event_id}),
            "v4/mm/get_markets",
        )
        return _require_dict_list(data, "markets", "v4/mm/get_markets")

    def get_multiple_markets(
        self, event_ids: Iterable[int]
    ) -> dict[str, list[dict[str, Any]]]:
        ids = _positive_unique_ids(event_ids, "event IDs")
        if not ids:
            return {}
        if len(ids) > 50:
            raise ValueError("ProphetX accepts at most 50 event IDs")
        data = _require_dict_data(
            self._request(
                "GET",
                "v4/mm/get_multiple_markets",
                params={"event_ids": ",".join(map(str, ids))},
            ),
            "v4/mm/get_multiple_markets",
        )
        result: dict[str, list[dict[str, Any]]] = {}
        for event_id, markets in data.items():
            if markets is None:
                result[str(event_id)] = []
                continue
            if not isinstance(markets, list) or not all(
                isinstance(market, dict) for market in markets
            ):
                raise ResponseError(
                    "multiple-market response contained invalid markets"
                )
            result[str(event_id)] = markets
        return result

    def get_price_ladder(self, *, refresh: bool = False) -> list[int]:
        with self._auth_lock:
            if self._price_ladder is not None and not refresh:
                return sorted(self._price_ladder)
            payload = self._request("GET", "v4/mm/get_price_ladder")
            data = payload.get("data")
            if (
                not isinstance(data, list)
                or not data
                or not all(
                    isinstance(price, int) and not isinstance(price, bool)
                    for price in data
                )
            ):
                raise ResponseError("price-ladder response contained invalid prices")
            for price in data:
                validate_american_odds(price)
            self._price_ladder = frozenset(data)
            return sorted(data)

    def get_strikes(self, strike_ids: Iterable[str]) -> list[dict[str, Any]]:
        ids = _unique_strings(strike_ids, "strike IDs")
        if not ids:
            return []
        if len(ids) > 20:
            raise ValueError("ProphetX accepts at most 20 strike IDs")
        payload = self._request(
            "GET",
            "v4/mm/get_strikes",
            params={"strike_ids": ",".join(ids)},
        )
        data = payload.get("data")
        if not isinstance(data, list) or not all(
            isinstance(strike, dict) for strike in data
        ):
            raise ResponseError("v4/mm/get_strikes response contained invalid data")
        return data

    def get_websocket_connection_config(self) -> dict[str, Any]:
        payload = self._request("GET", "websocket/connection-config")
        config = (
            payload.get("data") if isinstance(payload.get("data"), dict) else payload
        )
        if not isinstance(config, dict):
            raise ResponseError("websocket config was not an object")
        if not isinstance(config.get("key"), str) or not config["key"]:
            raise ResponseError("websocket config omitted key")
        if not (
            (isinstance(config.get("ws_host"), str) and config["ws_host"])
            or (isinstance(config.get("cluster"), str) and config["cluster"])
        ):
            raise ResponseError("websocket config omitted ws_host and cluster")
        return config

    def register_websocket(
        self,
        socket_id: str,
        event_ids: Iterable[int] = (),
        event_subtype_pairs: Iterable[tuple[int, str]] = (),
    ) -> dict[str, Any]:
        if not isinstance(socket_id, str) or not socket_id.strip():
            raise ValueError("socket_id cannot be empty")
        ids = _positive_unique_ids(event_ids, "event IDs")
        pairs = _normalize_event_subtype_pairs(event_subtype_pairs)
        subscriptions: list[dict[str, Any]] = []
        if ids:
            subscriptions.append({"type": "event", "ids": [str(item) for item in ids]})
        if pairs:
            subscriptions.append(
                {
                    "type": "event_subtype",
                    "ids": [f"{event_id}:{subtype}" for event_id, subtype in pairs],
                }
            )
        if not subscriptions:
            raise ValueError("at least one websocket subscription is required")
        payload = self._request(
            "POST",
            "v4/mm/websocket",
            json={"socket_id": socket_id, "subscriptions": subscriptions},
            idempotent=True,
        )
        data = _require_dict_data(payload, "v4/mm/websocket")
        _validate_registration(data, ids, pairs)
        return data

    def submit_multiple_orders(
        self, intents: Sequence[OrderIntent]
    ) -> dict[str, OrderResult]:
        if not intents:
            return {}
        if len(intents) > 20:
            raise ValueError("ProphetX accepts at most 20 orders per batch")
        external_ids = [intent.external_id for intent in intents]
        if len(external_ids) != len(set(external_ids)):
            raise ValueError("order external_id values must be unique")
        try:
            ladder = frozenset(self.get_price_ladder())
            payloads = [_order_payload(intent, ladder) for intent in intents]
            self._ensure_access_token()
        except ProphetXAPIError as exc:
            raise PreSubmitError("order preflight failed before submission") from exc
        except (TypeError, ValueError) as exc:
            raise PreSubmitError(str(exc)) from exc

        payload = self._request(
            "POST",
            "v4/mm/submit_multiple_orders",
            json={"data": payloads},
            ensure_access_token=False,
            mutation=True,
        )
        data = _require_dict_data(
            payload, "v4/mm/submit_multiple_orders", outcome_unknown=True
        )
        results = _parse_order_results(data, intents, payload)
        for intent in intents:
            results.setdefault(
                intent.external_id,
                OrderResult(
                    external_id=intent.external_id,
                    outcome="unknown",
                    status="unknown",
                    error_message="submission response omitted this order",
                ),
            )
        return results

    submit_orders = submit_multiple_orders

    def submit_order(self, intent: OrderIntent) -> OrderResult:
        try:
            ladder = frozenset(self.get_price_ladder())
            order = _order_payload(intent, ladder)
            self._ensure_access_token()
        except ProphetXAPIError as exc:
            raise PreSubmitError("order preflight failed before submission") from exc
        except (TypeError, ValueError) as exc:
            raise PreSubmitError(str(exc)) from exc

        payload = self._request(
            "POST",
            "v4/mm/submit_order",
            json=order,
            ensure_access_token=False,
            mutation=True,
        )
        data = _require_dict_data(
            payload,
            "v4/mm/submit_order",
            outcome_unknown=True,
        )
        accepted = data.get("order")
        if data.get("success") is not True or not isinstance(accepted, dict):
            raise _uncertain_order_response(payload, "invalid single-order result")
        external_id = accepted.get("external_id")
        order_id = accepted.get("order_id") or accepted.get("id")
        if (
            external_id not in {None, intent.external_id}
            or not isinstance(order_id, str)
            or not order_id
        ):
            raise _uncertain_order_response(
                payload,
                "conflicting or incomplete accepted order",
            )
        return OrderResult(
            external_id=intent.external_id,
            outcome="accepted",
            status=str(accepted.get("status") or "submitted"),
            payload=accepted,
            order_id=order_id,
        )

    def get_order(self, order_id: str) -> dict[str, Any]:
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("order_id cannot be empty")
        payload = self._request("GET", f"v4/mm/get_order/{order_id}")
        return _data_with_metadata(
            payload,
            "v4/mm/get_order/{id}",
            "last_synced_at",
        )

    def get_order_history_page(
        self,
        *,
        next_cursor: str | None = None,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
        updated_at_from: int | None = None,
        updated_at_to: int | None = None,
        limit: int = 100,
        market_id: str | None = None,
        matching_status: str | None = None,
        status: str | None = None,
        event_id: str | int | None = None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        history_bounds = (
            from_timestamp,
            to_timestamp,
            updated_at_from,
            updated_at_to,
        )
        if all(value is None for value in history_bounds):
            raise ValueError("order history requires at least one time boundary")
        if any(
            value is not None
            and (isinstance(value, bool) or not isinstance(value, int) or value <= 0)
            for value in history_bounds
        ):
            raise ValueError("order history time boundaries must be positive")
        _validate_time_range(
            from_timestamp,
            to_timestamp,
            maximum=_MAX_ORDER_HISTORY_CREATED_RANGE,
            label="order creation",
        )
        _validate_time_range(
            updated_at_from,
            updated_at_to,
            maximum=_MAX_ORDER_HISTORY_UPDATED_RANGE,
            label="order update",
        )
        params = {
            "next_cursor": next_cursor,
            "from": from_timestamp,
            "to": to_timestamp,
            "updated_at_from": updated_at_from,
            "updated_at_to": updated_at_to,
            "limit": limit,
            "market_id": market_id,
            "matching_status": matching_status,
            "status": status,
            "event_id": event_id,
        }
        payload = self._request(
            "GET",
            "v4/mm/get_order_history",
            params={key: value for key, value in params.items() if value is not None},
        )
        data = _data_with_metadata(
            payload,
            "v4/mm/get_order_history",
            "last_synced_at",
        )
        _require_dict_list(data, "orders", "v4/mm/get_order_history")
        cursor = data.get("next_cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise ResponseError("order-history next_cursor was not a string")
        return data

    def get_order_history(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return the orders from one history page for migration compatibility."""

        return self.get_order_history_page(**kwargs)["orders"]

    def get_order_matched_detail_page(
        self,
        *,
        order_id: str | None = None,
        order_ids: Iterable[str] = (),
        next_cursor: str | None = None,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        ids = _unique_strings(order_ids, "order IDs")
        if len(ids) > 100:
            raise ValueError("ProphetX accepts at most 100 order IDs")
        if order_id is not None and (
            not isinstance(order_id, str) or not order_id.strip()
        ):
            raise ValueError("order_id cannot be empty")
        _validate_optional_timestamps(from_timestamp, to_timestamp)
        _validate_time_range(
            from_timestamp,
            to_timestamp,
            maximum=None,
            label="matched detail",
        )
        params = {
            "order_id": order_id,
            "order_ids": ",".join(ids) if ids else None,
            "next_cursor": next_cursor,
            "from": from_timestamp,
            "to": to_timestamp,
            "limit": limit,
        }
        payload = self._request(
            "GET",
            "v4/mm/get_order_matched_detail",
            params={key: value for key, value in params.items() if value is not None},
        )
        return _require_cursor_page(
            payload,
            "v4/mm/get_order_matched_detail",
            "matching_details",
            metadata_keys=("last_synced_at",),
        )

    def get_trades_page(
        self,
        *,
        next_cursor: str | None = None,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        _validate_optional_timestamps(from_timestamp, to_timestamp)
        _validate_time_range(
            from_timestamp,
            to_timestamp,
            maximum=None,
            label="trade",
        )
        params = {
            "next_cursor": next_cursor,
            "from": from_timestamp,
            "to": to_timestamp,
            "limit": limit,
        }
        payload = self._request(
            "GET",
            "v4/mm/get_trades",
            params={key: value for key, value in params.items() if value is not None},
        )
        return _require_cursor_page(
            payload,
            "v4/mm/get_trades",
            "trades",
            metadata_keys=("last_synced_at",),
        )

    def get_transactions_page(
        self,
        *,
        next_cursor: str | None = None,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
        limit: int = 20,
        market_id: str | None = None,
        event_id: str | int | None = None,
        trade_id: str | None = None,
        transaction_type: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        _validate_optional_timestamps(from_timestamp, to_timestamp)
        _validate_time_range(
            from_timestamp,
            to_timestamp,
            maximum=None,
            label="transaction",
        )
        params = {
            "next_cursor": next_cursor,
            "from": from_timestamp,
            "to": to_timestamp,
            "limit": limit,
            "market_id": market_id,
            "event_id": event_id,
            "trade_id": trade_id,
            "transaction_type": transaction_type,
        }
        payload = self._request(
            "GET",
            "v4/mm/get_transactions",
            params={key: value for key, value in params.items() if value is not None},
        )
        return _require_cursor_page(
            payload,
            "v4/mm/get_transactions",
            "transactions",
        )

    def iter_order_history(
        self,
        *,
        max_pages: int = 100,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        cursor = kwargs.pop("next_cursor", None)
        seen: set[str] = set()
        for _ in range(max_pages):
            page = self.get_order_history_page(next_cursor=cursor, **kwargs)
            yield from page["orders"]
            next_cursor = page.get("next_cursor")
            if not next_cursor:
                return
            if next_cursor in seen:
                raise ResponseError("order-history cursor repeated")
            seen.add(next_cursor)
            cursor = next_cursor
        raise ResponseError(f"order history exceeded {max_pages} pages")

    def find_order_by_external_id(
        self,
        external_id: str,
        *,
        from_timestamp: int | None = None,
        updated_at_from: int | None = None,
        max_pages: int = 100,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        if not isinstance(external_id, str) or not external_id.strip():
            raise ValueError("external_id cannot be empty")
        if from_timestamp is None and updated_at_from is None:
            raise ValueError(
                "from_timestamp or updated_at_from is required for safe reconciliation"
            )
        if from_timestamp is not None and from_timestamp <= 0:
            raise ValueError("from_timestamp must be positive")
        if updated_at_from is not None and updated_at_from <= 0:
            raise ValueError("updated_at_from must be positive")
        for order in self.iter_order_history(
            from_timestamp=from_timestamp,
            updated_at_from=updated_at_from,
            max_pages=max_pages,
            **kwargs,
        ):
            if order.get("external_id") == external_id:
                return order
        return None

    def cancel_order(self, external_id: str, order_id: str) -> dict[str, Any]:
        if not isinstance(external_id, str) or not external_id.strip():
            raise ValueError("external_id cannot be empty")
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("order_id cannot be empty")
        payload = self._request(
            "POST",
            "v4/mm/cancel_order",
            json={"external_id": external_id, "order_id": order_id},
            mutation=True,
        )
        data = _require_dict_data(payload, "v4/mm/cancel_order", outcome_unknown=True)
        if data.get("success") is not True:
            raise ResponseError(
                "cancel response did not report success",
                raw_payload=payload,
                outcome_unknown=True,
            )
        return data

    cancel = cancel_order

    def cancel_multiple_orders(
        self,
        orders: Sequence[tuple[str, str]],
    ) -> list[dict[str, Any]]:
        if not orders:
            return []
        data: list[dict[str, str]] = []
        for order_id, external_id in orders:
            if not isinstance(order_id, str) or not order_id.strip():
                raise ValueError("order_id cannot be empty")
            if not isinstance(external_id, str) or not external_id.strip():
                raise ValueError("external_id cannot be empty")
            data.append({"order_id": order_id, "external_id": external_id})
        payload = self._request(
            "POST",
            "v4/mm/cancel_multiple_orders",
            json={"data": data},
            mutation=True,
        )
        result = payload.get("data")
        if not isinstance(result, list) or not all(
            isinstance(item, dict) for item in result
        ):
            raise ResponseError(
                "v4/mm/cancel_multiple_orders response contained invalid data",
                raw_payload=payload,
                outcome_unknown=True,
            )
        return result

    def cancel_all_orders(self) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "v4/mm/cancel_all_orders",
            mutation=True,
        )
        return _require_success_data(payload, "v4/mm/cancel_all_orders")

    def cancel_orders_by_event(self, event_id: int) -> dict[str, Any]:
        if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0:
            raise ValueError("event_id must be a positive integer")
        payload = self._request(
            "POST",
            "v4/mm/cancel_orders_by_event",
            json={"event_id": event_id},
            mutation=True,
        )
        return _require_success_data(payload, "v4/mm/cancel_orders_by_event")

    def cancel_orders_by_market(
        self,
        event_id: int,
        market_id: str,
        strike_id: str,
    ) -> dict[str, Any]:
        if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0:
            raise ValueError("event_id must be a positive integer")
        if not isinstance(market_id, str) or not market_id.strip():
            raise ValueError("market_id cannot be empty")
        if not isinstance(strike_id, str) or not strike_id.strip():
            raise ValueError("strike_id cannot be empty")
        payload = self._request(
            "POST",
            "v4/mm/cancel_orders_by_market",
            json={
                "event_id": event_id,
                "market_id": market_id,
                "strike_id": strike_id,
            },
            mutation=True,
        )
        return _require_success_data(payload, "v4/mm/cancel_orders_by_market")

    def _ensure_access_token(self) -> None:
        if self._access_token and self._clock() < self._refresh_at:
            return
        with self._auth_lock:
            if self._access_token and self._clock() < self._refresh_at:
                return
            if self._refresh_token:
                try:
                    self.refresh_session()
                    return
                except AuthenticationError:
                    pass
            self.authenticate()

    def _refresh_after_401(self, rejected_token: str | None) -> None:
        with self._auth_lock:
            if self._access_token and self._access_token != rejected_token:
                return
            try:
                self.refresh_session()
            except AuthenticationError:
                self.authenticate()

    def _request(
        self,
        method: str,
        path: str,
        *,
        authenticated: bool = True,
        retry_auth: bool = True,
        ensure_access_token: bool = True,
        mutation: bool = False,
        idempotent: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("ProphetX client is closed")
        path = path.lstrip("/")
        method = method.upper()
        if idempotent is None:
            idempotent = method in {"GET", "HEAD", "OPTIONS"}
        if authenticated and ensure_access_token:
            self._ensure_access_token()
        retries = 0
        while True:
            self.scheduler.acquire(path, market_query=path in _SPACED_QUERY_PATHS)
            token = self._access_token if authenticated else None
            headers = dict(kwargs.pop("headers", {}))
            if token:
                headers["Authorization"] = f"Bearer {token}"
            try:
                response = self._client.request(method, path, headers=headers, **kwargs)
            except httpx.HTTPError as exc:
                raise TransportError(
                    f"ProphetX {method} {path} transport failed",
                    details={"exception": type(exc).__name__},
                    outcome_unknown=mutation,
                ) from exc

            try:
                payload = orjson.loads(response.content) if response.content else None
            except orjson.JSONDecodeError:
                if response.is_success:
                    raise ResponseError(
                        f"ProphetX {method} {path} returned invalid JSON",
                        status=response.status_code,
                        details={"response_text": response.text[:1000]},
                        outcome_unknown=mutation,
                    ) from None
                payload = {"response_text": response.text[:1000]}
            if response.status_code == 401 and authenticated and retry_auth:
                self._refresh_after_401(token)
                return self._request(
                    method,
                    path,
                    authenticated=True,
                    retry_auth=False,
                    ensure_access_token=False,
                    mutation=mutation,
                    idempotent=idempotent,
                    **kwargs,
                )
            if response.status_code == 429:
                delay = _retry_delay(
                    response,
                    payload,
                    retries,
                    wall_clock=self._wall_clock,
                    random_source=self._random,
                    policy=self.scheduler.policy,
                )
                self.scheduler.cooldown(path, delay, global_=True)
                if (
                    idempotent
                    and not mutation
                    and retries < self.scheduler.policy.max_429_retries
                ):
                    retries += 1
                    continue
            if not response.is_success:
                error_type = (
                    AuthenticationError
                    if response.status_code == 401
                    else ProphetXAPIError
                )
                code, details = _error_details(payload)
                raise error_type(
                    f"ProphetX {method} {path} failed with HTTP {response.status_code}",
                    status=response.status_code,
                    code=code,
                    details=details,
                    raw_payload=payload,
                    outcome_unknown=mutation
                    and (response.status_code == 408 or response.status_code >= 500),
                )
            if not isinstance(payload, dict):
                raise ResponseError(
                    f"ProphetX {method} {path} returned invalid JSON object",
                    status=response.status_code,
                    raw_payload=payload,
                    outcome_unknown=mutation,
                )
            return payload

    def _set_refresh_deadline(self, raw_expiry: Any) -> None:
        lifetime = _DEFAULT_ACCESS_LIFETIME
        if isinstance(raw_expiry, int | float) and not isinstance(raw_expiry, bool):
            expiry = float(raw_expiry)
            if expiry > 1e18:
                expiry /= 1e9
            elif expiry > 1e15:
                expiry /= 1e6
            elif expiry > 1e12:
                expiry /= 1e3
            if expiry > self._wall_clock():
                lifetime = expiry - self._wall_clock()
            elif 0 < expiry <= 86400:
                lifetime = expiry
        self._refresh_at = self._clock() + max(1.0, lifetime - _REFRESH_MARGIN)


def _normalize_environment(environment: str | Environment) -> Environment:
    if isinstance(environment, Environment):
        return environment
    if not isinstance(environment, str):
        raise TypeError("environment must be a string")
    try:
        return Environment(environment.strip().lower())
    except ValueError:
        raise ValueError(
            "environment must be exactly 'sandbox' or 'production'"
        ) from None


def _positive_unique_ids(values: Iterable[int], label: str) -> tuple[int, ...]:
    result = tuple(dict.fromkeys(values))
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in result
    ):
        raise ValueError(f"{label} must be positive integers")
    return result


def _unique_strings(values: Iterable[str], label: str) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(values))
    if any(not isinstance(value, str) or not value.strip() for value in result):
        raise ValueError(f"{label} must be non-empty strings")
    return result


def _normalize_event_subtype_pairs(
    pairs: Iterable[tuple[int, str]],
) -> tuple[tuple[int, str], ...]:
    result: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()
    for event_id, subtype in pairs:
        if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0:
            raise ValueError("event subtype event IDs must be positive integers")
        if not isinstance(subtype, str) or not subtype.strip():
            raise ValueError("event subtype cannot be empty")
        pair = (event_id, canonicalize_subtype(subtype))
        if pair not in seen:
            result.append(pair)
            seen.add(pair)
    return tuple(result)


def _validate_registration(
    data: dict[str, Any],
    event_ids: tuple[int, ...],
    pairs: tuple[tuple[int, str], ...],
) -> None:
    if data.get("success") is not True:
        raise ResponseError(
            "websocket registration did not report success", raw_payload=data
        )
    status = data.get("status")
    if not isinstance(status, str) or status.lower() not in {
        "connected",
        "success",
        "ok",
        "authenticated",
        "registered",
    }:
        raise ResponseError("websocket registration status was not successful")
    rejected = data.get("rejected")
    if rejected is None:
        rejected = []
    if not isinstance(rejected, list):
        raise ResponseError("websocket registration omitted rejected list")
    if rejected:
        raise ResponseError(
            "websocket registration rejected subscriptions",
            details=rejected,
            raw_payload=data,
        )
    channels = data.get("authorized_channel")
    if not isinstance(channels, list) or not all(
        isinstance(channel, dict) for channel in channels
    ):
        raise ResponseError("websocket registration omitted authorized channels")
    count = data.get("channel_count")
    limit = data.get("channel_limit")
    expected_count = len(event_ids) + len(pairs)
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or isinstance(limit, bool)
        or not isinstance(limit, int)
        or count != expected_count
        or count > limit
    ):
        raise ResponseError("websocket registration channel counts were invalid")
    scopes = {
        (
            scope.get("event_id"),
            canonicalize_subtype(scope["sub_type"])
            if isinstance(scope.get("sub_type"), str) and scope["sub_type"].strip()
            else None,
        )
        for channel in channels
        if isinstance((scope := channel.get("scope")), dict)
    }
    missing_events = {(event_id, None) for event_id in event_ids} - scopes
    missing_pairs = set(pairs) - scopes
    if missing_events or missing_pairs:
        raise ResponseError(
            "websocket registration omitted required scopes",
            details={
                "missing_events": sorted(missing_events),
                "missing_event_subtypes": sorted(missing_pairs),
            },
        )


def _error_details(payload: Any) -> tuple[str | int | None, Any]:
    if not isinstance(payload, dict):
        return None, payload
    code = payload.get("code", payload.get("error"))
    details = payload.get("details", payload.get("message", payload))
    if isinstance(code, dict | list):
        code = None
    return code, details


def _require_dict_data(
    payload: dict[str, Any],
    endpoint: str,
    *,
    outcome_unknown: bool = False,
) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ResponseError(
            f"{endpoint} response omitted data object",
            raw_payload=payload,
            outcome_unknown=outcome_unknown,
        )
    return data


def _data_with_metadata(
    payload: dict[str, Any],
    endpoint: str,
    *metadata_keys: str,
) -> dict[str, Any]:
    data = dict(_require_dict_data(payload, endpoint))
    for key in metadata_keys:
        if key in payload:
            data[key] = payload[key]
    return data


def _require_cursor_page(
    payload: dict[str, Any],
    endpoint: str,
    item_key: str,
    *,
    metadata_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    data = _data_with_metadata(payload, endpoint, *metadata_keys)
    _require_dict_list(data, item_key, endpoint)
    cursor = data.get("next_cursor")
    if cursor is not None and not isinstance(cursor, str):
        raise ResponseError(f"{endpoint} next_cursor was not a string")
    return data


def _require_success_data(
    payload: dict[str, Any],
    endpoint: str,
) -> dict[str, Any]:
    data = _require_dict_data(payload, endpoint, outcome_unknown=True)
    if data.get("success") is not True:
        raise ResponseError(
            f"{endpoint} response did not report success",
            raw_payload=payload,
            outcome_unknown=True,
        )
    return data


def _require_dict_list(
    data: dict[str, Any], key: str, endpoint: str
) -> list[dict[str, Any]]:
    values = data.get(key)
    if not isinstance(values, list) or not all(
        isinstance(value, dict) for value in values
    ):
        raise ResponseError(f"{endpoint} response contained invalid {key}")
    return values


def _order_payload(intent: OrderIntent, ladder: frozenset[int]) -> dict[str, Any]:
    if not isinstance(intent.external_id, str) or not intent.external_id.strip():
        raise ValueError("external_id cannot be empty")
    if not isinstance(intent.strike_id, str) or not intent.strike_id.strip():
        raise ValueError("strike_id cannot be empty")
    validate_american_odds(intent.price)
    if intent.price not in ladder:
        raise ValueError("price is not present in the ProphetX price ladder")
    if isinstance(intent.quantity, bool) or not isinstance(
        intent.quantity, int | float
    ):
        raise TypeError("quantity must be numeric")
    if intent.quantity <= 0:
        raise ValueError("quantity must be positive")
    if intent.order_strategy is not None and intent.order_strategy not in {
        "fillOrKill",
        "ALO",
    }:
        raise ValueError("order_strategy must be fillOrKill, ALO, or None")
    payload = {
        "external_id": intent.external_id,
        "strike_id": intent.strike_id,
        "price": intent.price,
        "quantity": round(float(intent.quantity), 2),
    }
    if intent.order_strategy is not None:
        payload["order_strategy"] = intent.order_strategy
    return payload


def _validate_time_range(
    start: int | None,
    end: int | None,
    *,
    maximum: int | None,
    label: str,
) -> None:
    if start is None or end is None:
        return
    if start > end:
        raise ValueError(f"{label} range start cannot be after its end")
    if maximum is not None and end - start > maximum:
        days = maximum // (24 * 60 * 60)
        raise ValueError(f"{label} range cannot exceed {days} days")


def _validate_optional_timestamps(*values: int | None) -> None:
    if any(
        value is not None
        and (isinstance(value, bool) or not isinstance(value, int) or value <= 0)
        for value in values
    ):
        raise ValueError("timestamps must be positive integers")


def _parse_order_results(
    data: dict[str, Any],
    intents: Sequence[OrderIntent],
    payload: dict[str, Any],
) -> dict[str, OrderResult]:
    succeeded = data.get("succeed_orders", [])
    failed = data.get("failed_orders", [])
    if not isinstance(succeeded, list) or not isinstance(failed, list):
        raise _uncertain_order_response(payload, "invalid result lists")

    requested = {intent.external_id for intent in intents}
    results: dict[str, OrderResult] = {}
    for item in succeeded:
        if not isinstance(item, dict):
            raise _uncertain_order_response(payload, "invalid accepted order")
        external_id = item.get("external_id")
        order_id = item.get("order_id") or item.get("id")
        if (
            not isinstance(external_id, str)
            or external_id not in requested
            or external_id in results
            or not isinstance(order_id, str)
            or not order_id
        ):
            raise _uncertain_order_response(
                payload,
                "conflicting or incomplete accepted order",
            )
        results[external_id] = OrderResult(
            external_id=external_id,
            outcome="accepted",
            status=str(item.get("status") or "submitted"),
            payload=item,
            order_id=order_id,
        )

    for item in failed:
        if not isinstance(item, dict):
            raise _uncertain_order_response(payload, "invalid rejected order")
        request = item.get("request")
        request_external_id = (
            request.get("external_id") if isinstance(request, dict) else None
        )
        index = item.get("index")
        indexed_external_id = (
            intents[index].external_id
            if isinstance(index, int)
            and not isinstance(index, bool)
            and 0 <= index < len(intents)
            else None
        )
        if (
            request_external_id is not None
            and request_external_id != indexed_external_id
        ):
            raise _uncertain_order_response(
                payload,
                "rejected order index and external_id disagree",
            )
        external_id = request_external_id or indexed_external_id
        if (
            not isinstance(external_id, str)
            or external_id not in requested
            or external_id in results
        ):
            raise _uncertain_order_response(
                payload,
                "conflicting or unidentified rejected order",
            )
        results[external_id] = OrderResult(
            external_id=external_id,
            outcome="rejected",
            status="rejected",
            payload=item,
            error_code=_string_or_none(item.get("error")),
            error_message=_string_or_none(item.get("message")),
        )
    return results


def _uncertain_order_response(
    payload: dict[str, Any],
    detail: str,
) -> ResponseError:
    return ResponseError(
        f"order response contained {detail}",
        raw_payload=payload,
        outcome_unknown=True,
    )


def _string_or_none(value: Any) -> str | None:
    return str(value) if value is not None and str(value) else None


def _retry_delay(
    response: httpx.Response,
    payload: Any,
    attempt: int,
    *,
    wall_clock: Any,
    random_source: Any,
    policy: RateLimitPolicy,
) -> float:
    header = response.headers.get("Retry-After")
    if header:
        try:
            return max(0.0, float(header))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(header)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                return max(0.0, parsed.timestamp() - wall_clock())
            except TypeError, ValueError, OverflowError:
                pass
    timestamp = _find_reset_timestamp(payload)
    if timestamp is not None:
        return max(0.0, timestamp - wall_clock())
    base = min(
        policy.fallback_backoff_cap,
        policy.fallback_backoff_base * (2**attempt),
    )
    return base * (0.5 + float(random_source()) * 0.5)


def _find_reset_timestamp(value: Any) -> float | None:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if any(part in lowered for part in ("next_allow", "reset", "retry_at")):
                parsed = _parse_timestamp(item)
                if parsed is not None:
                    return parsed
            parsed = _find_reset_timestamp(item)
            if parsed is not None:
                return parsed
    elif isinstance(value, list):
        for item in value:
            parsed = _find_reset_timestamp(item)
            if parsed is not None:
                return parsed
    return None


def _parse_timestamp(value: Any) -> float | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        result = float(value)
        if result > 1e18:
            result /= 1e9
        elif result > 1e15:
            result /= 1e6
        elif result > 1e12:
            result /= 1e3
        return result if result > 1e9 else None
    if isinstance(value, str):
        try:
            return _parse_timestamp(float(value))
        except ValueError:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
    return None
