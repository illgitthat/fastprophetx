from __future__ import annotations

import copy
import random
import threading
from collections.abc import Iterable
from typing import Any

import pytest

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


def price_update(market_id: int, strike_id: str, price: int) -> dict[str, Any]:
    return {
        "market_selections": [
            {
                "market_id": market_id,
                "strike_id": strike_id,
                "outcome_id": 7,
                "price": price,
                "quantity": 4,
            }
        ]
    }


def test_repeated_reads_reflect_only_later_updates() -> None:
    class Client:
        def get_multiple_markets(
            self,
            _event_ids: object,
        ) -> dict[str, list[dict[str, Any]]]:
            return {
                "1": [market(10, "strike-1", -110)],
                "2": [market(20, "strike-2", 105)],
            }

    store = MarketStore(Client())
    store.refresh([1, 2])
    first = store.markets([1, 2])
    first_copy = copy.deepcopy(first)

    assert store.markets([1, 2]) == first

    store.cache.apply_update(1, price_update(10, "strike-1", 120))
    updated = store.markets([1, 2])

    assert updated[1][0]["selections"][0][0]["price"] == 120
    assert updated[2] == first[2]
    assert first == first_copy


def test_invalidated_event_is_not_served_from_previous_read() -> None:
    class Client:
        def __init__(self) -> None:
            self.prices = [-110, 150]

        def get_multiple_markets(
            self,
            _event_ids: object,
        ) -> dict[str, list[dict[str, Any]]]:
            return {"1": [market(10, "strike-1", self.prices.pop(0))]}

    store = MarketStore(Client())
    store.refresh([1])
    assert store.markets([1])[1][0]["selections"][0][0]["price"] == -110

    store.invalidate_events([1])

    assert store.markets([1])[1][0]["selections"][0][0]["price"] == 150


@pytest.mark.parametrize("seed", range(25))
def test_cached_reads_match_fresh_materialization(seed: int) -> None:
    rng = random.Random(seed)
    events = (1, 2, 3)
    prices = (-200, -150, -110, 100, 105, 120, 150)
    selections = [
        (market_id, f"{market_id}-{strike}", outcome)
        for market_id, strikes in ((10, 1), (20, 1), (30, 3))
        for strike in range(strikes)
        for outcome in (7, 8)
    ]

    def rest_market(market_id: int) -> dict[str, Any]:
        def levels(strike_id: str, outcome: int) -> list[dict[str, Any]]:
            return [
                {
                    "strike_id": strike_id,
                    "outcome_id": outcome,
                    "price": price,
                    "quantity": rng.randint(1, 5),
                }
                for price in rng.sample(prices, rng.randint(0, 2))
            ]

        strikes = [
            {
                "strike_id": strike_id,
                "selections": [levels(strike_id, 7), levels(strike_id, 8)],
            }
            for selected_market, strike_id, outcome in selections
            if selected_market == market_id and outcome == 7
        ]
        if market_id == 30:
            return {"id": market_id, "sub_type": "spread", "market_strikes": strikes}
        return {"id": market_id, "sub_type": "total", **strikes[0]}

    def rest_markets() -> list[dict[str, Any]]:
        return [rest_market(item) for item in (10, 20, 30) if rng.random() < 0.8]

    class Client:
        def get_multiple_markets(
            self,
            event_ids: Iterable[int],
        ) -> dict[str, list[dict[str, Any]]]:
            response: dict[str, list[dict[str, Any]]] = {}
            for event_id in event_ids:
                roll = rng.random()
                if roll < 0.7:
                    response[str(event_id)] = rest_markets()
                elif roll < 0.85:
                    response[str(event_id)] = []
            return response

    def live_update() -> dict[str, Any]:
        market_id, strike_id, outcome = rng.choice(selections)
        level = {
            "market_id": market_id,
            "strike_id": strike_id,
            "outcome_id": outcome,
            "price": rng.choice(prices),
            "quantity": rng.choice((0, 1, 3)),
        }
        if rng.random() < 0.2:
            level["event_id"] = rng.choice(events)
        return {"market_selections": [level]}

    store = MarketStore(Client(), reconcile_seconds=float("inf"))
    store.refresh(events)
    reads: list[tuple[object, object]] = []
    for _ in range(150):
        event_id = rng.choice(events)
        operation = rng.randrange(9)
        if operation <= 1:
            store.cache.apply_update(event_id, live_update())
        elif operation == 2:
            store.cache.apply_update(
                event_id,
                {"markets": rest_markets()},
                replace=True,
            )
        elif operation == 3:
            store.invalidate_events([event_id])
        elif operation == 4:
            store.refresh(rng.sample(events, rng.randint(1, 3)))
        elif operation == 5:
            store.clear_event(event_id)
            store.refresh([event_id])
        elif operation == 6:
            store.cache.reconcile(store.client, [event_id])
        elif operation == 7:
            store.cache.clear_event(event_id)
        else:
            ids = rng.sample(events, rng.randint(1, 3))
            first = store.markets(ids)
            repeated = store.markets(ids)
            store._materialized.clear()
            assert store.markets(ids) == first == repeated
            reads.append((first, copy.deepcopy(first)))
    assert all(read == snapshot for read, snapshot in reads)
