from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any

from fastprophetx import MarketStore


def market(market_id: int, strike_id: str, price: int) -> dict[str, Any]:
    return {
        "id": market_id,
        "sub_type": "total",
        "status": "active",
        "strike": 40.5,
        "selections": [
            [
                {
                    "strike_id": strike_id,
                    "outcome_id": 7,
                    "name": "Over 40.5",
                    "price": price,
                    "quantity": 5,
                }
            ]
        ],
    }


def test_live_depth_omission_repopulation_and_explicit_pruning() -> None:
    class Client:
        def __init__(self) -> None:
            self.responses = [
                {
                    "1": [
                        market(10, "strike-1", -110),
                        market(20, "strike-2", 105),
                    ]
                },
                {},
                {"1": [market(10, "strike-1", -105)]},
            ]
            self.calls = 0

        def get_multiple_markets(
            self,
            _event_ids: object,
        ) -> dict[str, list[dict[str, Any]]]:
            self.calls += 1
            return self.responses.pop(0)

    client = Client()
    store = MarketStore(client)
    store.refresh([1])
    store.cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 20,
                    "strike_id": "strike-2",
                    "outcome_id": 7,
                    "price": 120,
                    "quantity": 3,
                }
            ]
        },
    )
    live = store.markets([1])
    assert live[1][1]["selections"][0][0]["price"] == 120
    assert client.calls == 1

    assert store.refresh([1])[1] == []
    store.cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 20,
                    "strike_id": "strike-2",
                    "outcome_id": 7,
                    "price": 125,
                    "quantity": 2,
                }
            ]
        },
    )
    assert store.markets([1])[1][0]["id"] == 20

    store.refresh([1])
    store.cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 20,
                    "strike_id": "strike-2",
                    "outcome_id": 7,
                    "price": 130,
                    "quantity": 1,
                }
            ]
        },
    )
    assert [item["id"] for item in store.markets([1])[1]] == [10]


def test_invalid_books_require_authoritative_refresh() -> None:
    class Client:
        calls = 0

        def get_multiple_markets(
            self,
            _event_ids: object,
        ) -> dict[str, list[dict[str, Any]]]:
            self.calls += 1
            return {"1": [market(10, "strike-1", -110)]}

    client = Client()
    store = MarketStore(client)
    store.refresh([1])
    store.cache.invalidate_events([1])

    refreshed = store.markets([1])

    assert refreshed[1][0]["selections"][0][0]["price"] == -110
    assert client.calls == 2
    assert store.cache.books_valid([1]) is True


def test_pre_invalidation_rest_response_cannot_restore_book_validity() -> None:
    started = threading.Event()
    release = threading.Event()

    class Client:
        calls = 0

        def get_multiple_markets(
            self,
            _event_ids: object,
        ) -> dict[str, list[dict[str, Any]]]:
            self.calls += 1
            if self.calls == 1:
                started.set()
                assert release.wait(5)
            return {"1": [market(10, "strike-1", -110)]}

    client = Client()
    store = MarketStore(client)
    result: dict[int, list[dict[str, Any]]] = {}
    worker = threading.Thread(target=lambda: result.update(store.refresh([1])))
    worker.start()
    assert started.wait(5)
    store.invalidate_events([1])
    release.set()
    worker.join(5)

    assert result[1] == []
    assert store.cache.books_valid([1]) is False
    assert store.refresh([1])[1][0]["selections"][0][0]["price"] == -110
    assert store.cache.books_valid([1]) is True


def test_concurrent_markets_calls_share_one_refresh() -> None:
    started = threading.Event()
    release = threading.Event()
    stale_callers = threading.Barrier(3)

    class Client:
        calls = 0

        def get_multiple_markets(
            self,
            _event_ids: object,
        ) -> dict[str, list[dict[str, Any]]]:
            self.calls += 1
            started.set()
            assert release.wait(5)
            return {"1": [market(10, "strike-1", -110)]}

    class ObservedStore(MarketStore):
        def refresh(
            self,
            event_ids: Iterable[int],
            *,
            force: bool = True,
            websocket_ready: bool = True,
        ) -> dict[int, list[dict[str, Any]]]:
            if not force:
                stale_callers.wait()
            return super().refresh(
                event_ids,
                force=force,
                websocket_ready=websocket_ready,
            )

    client = Client()
    store = ObservedStore(client)
    results: list[dict[int, list[dict[str, Any]]]] = []
    workers = [
        threading.Thread(target=lambda: results.append(store.markets([1])))
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    stale_callers.wait()
    assert started.wait(5)
    release.set()
    for worker in workers:
        worker.join(5)

    assert client.calls == 1
    assert len(results) == 2
    assert all(result[1][0]["id"] == 10 for result in results)
