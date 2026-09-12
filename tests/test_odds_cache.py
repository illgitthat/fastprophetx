from __future__ import annotations

import threading
from collections.abc import Iterable

import pytest

from fastprophetx import (
    MarketCache,
    implied_probability,
    normalize_selection_levels,
    profit_to_stake,
    stake_to_profit,
    validate_american_odds,
)


@pytest.mark.parametrize("odds", [-99, -1, 0, 1, 99])
def test_invalid_american_odds(odds: int) -> None:
    with pytest.raises(ValueError):
        validate_american_odds(odds)


def test_odds_math() -> None:
    assert implied_probability(-110) == pytest.approx(110 / 210)
    assert implied_probability(200) == pytest.approx(1 / 3)
    assert stake_to_profit(110, -110) == pytest.approx(100)
    assert profit_to_stake(100, -110) == pytest.approx(110)
    assert stake_to_profit(50, 200) == pytest.approx(100)
    assert profit_to_stake(100, 200) == pytest.approx(50)


def test_depth_quantity_primary_and_strict_mismatch() -> None:
    raw = {"price": -110, "quantity": 4, "value": 3.64, "extra": "kept"}
    level = normalize_selection_levels([raw])[0]
    assert level.quantity == 4
    assert level.value == 3.64
    assert level.inconsistent is True
    assert level.raw == raw
    with pytest.raises(ValueError, match="inconsistent"):
        normalize_selection_levels([raw], strict=True)


def test_strict_update_failure_is_transactional_and_sequence_retryable() -> None:
    cache = MarketCache(strict_depth=True)
    payload = {
        "sequence": 5,
        "market_selections": [
            {
                "market_id": 10,
                "strike_id": "s1",
                "outcome_id": 1,
                "price": 105,
                "quantity": 2,
                "value": 2,
            },
            {
                "market_id": 10,
                "strike_id": "s2",
                "outcome_id": 2,
                "price": 120,
                "quantity": 3,
                "value": 4,
            },
        ],
    }
    with pytest.raises(ValueError, match="inconsistent"):
        cache.apply_update(1, payload)
    assert cache.event_snapshot(1).known_keys == frozenset()

    payload["market_selections"][1]["value"] = 3
    assert cache.apply_update(1, payload) is True
    assert len(cache.event_snapshot(1).known_keys) == 2


@pytest.mark.parametrize("invalid_price", [100.5, float("nan"), float("inf")])
def test_invalid_deletion_does_not_advance_sequence_or_change_depth(
    invalid_price: float,
) -> None:
    cache = MarketCache()
    key = (1, "10", "s", "1")
    cache.apply_update(
        1,
        {
            "sequence": 1,
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 1,
                    "price": 100,
                    "quantity": 2,
                }
            ],
        },
    )
    with pytest.raises(ValueError, match="deletion price"):
        cache.apply_update(
            1,
            {
                "sequence": 2,
                "market_selections": [
                    {
                        "market_id": 10,
                        "strike_id": "s",
                        "outcome_id": 1,
                        "price": invalid_price,
                        "quantity": 0,
                    }
                ],
            },
        )
    assert cache.current_levels(key)[0].quantity == 2
    assert cache.apply_update(
        1,
        {
            "sequence": 2,
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 1,
                    "price": 100,
                    "quantity": 0,
                }
            ],
        },
    )
    assert cache.current_levels(key) == ()


def test_nested_v4_depth_stale_sequence_and_delete() -> None:
    cache = MarketCache()
    payload = {
        "sequence": 5,
        "markets": [
            {
                "id": 10,
                "sub_type": "moneyline",
                "selections": [
                    [
                        {
                            "strike_id": "s1",
                            "outcome_id": 7,
                            "price": -110,
                            "quantity": 5,
                            "value": 4,
                        },
                        {
                            "strike_id": "s1",
                            "outcome_id": 7,
                            "price": 105,
                            "quantity": 2,
                            "value": 2,
                        },
                    ]
                ],
            }
        ],
    }
    assert cache.apply_update(1, payload) is True
    key = (1, "10", "s1", "7")
    assert [level.price for level in cache.current_levels(key)] == [105, -110]
    assert cache.apply_update(1, {**payload, "sequence": 5}) is False
    assert (
        cache.apply_update(
            1,
            {
                "sequence": 6,
                "market_selections": [
                    {
                        "market_id": 10,
                        "strike_id": "s1",
                        "outcome_id": 7,
                        "price": 105,
                        "quantity": 0,
                    }
                ],
            },
        )
        is True
    )
    assert [level.price for level in cache.current_levels(key)] == [-110]
    cache.apply_update(
        1,
        {
            "sequence": 7,
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s1",
                    "outcome_id": 7,
                    "price": -110,
                    "quantity": 0,
                }
            ],
        },
    )
    assert cache.snapshot(key) is None
    assert cache.has_key(key) is True
    assert key in cache.known_keys(event_id=1)


def test_empty_selection_container_deletes_liquidity() -> None:
    cache = MarketCache()
    cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 2,
                    "price": 100,
                    "quantity": 3,
                }
            ]
        },
    )
    key = (1, "10", "s", "2")
    assert cache.snapshot(key) is not None
    assert cache.apply_update(
        1,
        {
            "market_id": 10,
            "strike_id": "s",
            "outcome_id": 2,
            "selections": [],
        },
    )
    assert cache.snapshot(key) is None


def test_event_snapshot_captures_depth_and_tombstones_atomically() -> None:
    cache = MarketCache()
    live_key = (1, "10", "s", "1")
    empty_key = (1, "10", "s", "2")
    cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 1,
                    "price": 105,
                    "quantity": 2,
                }
            ],
            "updates": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 2,
                    "selections": [],
                }
            ],
        },
    )

    snapshot = cache.event_snapshot(1)

    assert snapshot.known_keys == frozenset({live_key, empty_key})
    assert snapshot.snapshots[live_key].levels[0].price == 105
    assert empty_key not in snapshot.snapshots


def test_authoritative_rest_repairs_old_state_but_preserves_newer_ws() -> None:
    now = [10.0]
    cache = MarketCache(
        monotonic=lambda: now[0],
        wall_clock=lambda: 1_700_000_000.0 + now[0],
    )
    key = (1, "10", "s", "2")
    cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 2,
                    "price": -110,
                    "quantity": 3,
                },
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 2,
                    "price": 100,
                    "quantity": 2,
                },
            ]
        },
    )
    now[0] = 20.0
    request = cache.begin_reconcile([1])
    now[0] = 30.0
    cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 2,
                    "price": 100,
                    "quantity": 7,
                }
            ]
        },
    )
    now[0] = 40.0
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
                                "outcome_id": 2,
                                "price": 105,
                                "quantity": 4,
                            }
                        ]
                    ],
                }
            ]
        },
        request=request,
    )
    levels = cache.current_levels(key)
    assert [level.price for level in levels] == [105, 100]
    assert {level.price: level.quantity for level in levels} == {
        105: 4,
        100: 7,
    }

    assert cache.clear_event(1) == 1
    assert cache.has_key(key) is False
    assert cache.current_levels(key) == ()


@pytest.mark.parametrize("omit_before_request", [False, True])
def test_newer_replace_omission_survives_older_rest_response(
    omit_before_request: bool,
) -> None:
    now = [1.0]
    cache = MarketCache(monotonic=lambda: now[0])
    key = (1, "10", "s", "1")
    cache.apply_update(
        1,
        {
            "markets": [
                {
                    "id": 10,
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
        replace=True,
    )
    now[0] = 2.0
    if omit_before_request:
        assert cache.apply_update(1, {"markets": []}, replace=True) is True
        assert cache.has_key(key) is False
    now[0] = 3.0
    request = cache.begin_reconcile([1])
    now[0] = 3.5
    assert cache.apply_update(
        1,
        {
            "market_id": 10,
            "strike_id": "s",
            "outcome_id": 1,
            "selections": [],
        },
    )
    assert cache.has_key(key) is True
    now[0] = 4.0
    assert cache.apply_update(1, {"markets": []}, replace=True) is True
    now[0] = 5.0
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
                                "outcome_id": 1,
                                "price": 120,
                                "quantity": 9,
                            },
                            {
                                "strike_id": "new",
                                "outcome_id": 2,
                                "price": -110,
                                "quantity": 5,
                            },
                        ]
                    ],
                }
            ]
        },
        request=request,
    )
    assert cache.has_key(key) is False
    assert cache.current_levels(key) == ()
    assert cache.current_levels((1, "10", "new", "2")) == ()
    assert cache.wait_for_update(
        after_monotonic=request.started_at_monotonic,
        timeout=0,
        event_id=1,
    )


def test_post_request_selection_layout_survives_older_empty_rest() -> None:
    now = [1.0]
    cache = MarketCache(monotonic=lambda: now[0])
    key = (1, "10", "s", "1")
    request = cache.begin_reconcile([1])
    now[0] = 2.0
    cache.apply_update(
        1,
        {
            "sport_event_id": 1,
            "market_id": 10,
            "info": {
                "sequence_number": 1,
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
            },
        },
    )
    now[0] = 3.0
    cache.apply_authoritative_snapshot(
        1,
        {"markets": []},
        request=request,
    )
    assert cache.current_levels(key)[0].price == 105

    now[0] = 4.0
    cache.apply_update(
        1,
        {
            "sport_event_id": 1,
            "market_id": 10,
            "info": {
                "sequence_number": 2,
                "selections": [[]],
            },
        },
    )
    assert cache.current_levels(key) == ()


def test_reconcile_clears_requested_event_omitted_from_response() -> None:
    now = [1.0]
    cache = MarketCache(monotonic=lambda: now[0])
    key = (1, "10", "s", "2")
    cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s",
                    "outcome_id": 2,
                    "price": 100,
                    "quantity": 3,
                }
            ]
        },
    )

    class Client:
        def get_multiple_markets(self, _event_ids: object) -> dict[str, object]:
            now[0] = 2.0
            return {}

    assert cache.reconcile(Client(), [1]) == 1
    assert cache.has_key(key) is False
    assert cache.current_levels(key) == ()


def test_overlapping_reconcile_does_not_resurrect_newer_omission() -> None:
    old_started = threading.Event()
    release_old = threading.Event()
    cache = MarketCache()
    event_one_key = (1, "10", "s1", "1")
    event_two_key = (2, "20", "s2", "1")
    cache.apply_update(
        1,
        {
            "market_selections": [
                {
                    "market_id": 10,
                    "strike_id": "s1",
                    "outcome_id": 1,
                    "price": -110,
                    "quantity": 1,
                }
            ]
        },
    )

    def markets(event_id: int, price: int) -> list[dict[str, object]]:
        return [
            {
                "id": event_id * 10,
                "selections": [
                    [
                        {
                            "strike_id": f"s{event_id}",
                            "outcome_id": 1,
                            "price": price,
                            "quantity": 2,
                        }
                    ]
                ],
            }
        ]

    class Client:
        def get_multiple_markets(
            self,
            event_ids: Iterable[int],
        ) -> dict[str, list[dict[str, object]]]:
            ids = tuple(event_ids)
            if ids == (1, 2):
                old_started.set()
                assert release_old.wait(5)
                return {"1": markets(1, -120), "2": markets(2, 105)}
            assert ids == (1,)
            return {}

    client = Client()
    older = threading.Thread(target=cache.reconcile, args=(client, (1, 2)))
    newer = threading.Thread(target=cache.reconcile, args=(client, (1,)))
    older.start()
    assert old_started.wait(5)
    newer.start()
    newer.join(5)
    assert not newer.is_alive()
    release_old.set()
    older.join(5)
    assert not older.is_alive()

    assert cache.has_key(event_one_key) is False
    assert cache.current_levels(event_one_key) == ()
    assert [level.price for level in cache.current_levels(event_two_key)] == [105]


def test_cache_callbacks_wait_and_seed_once() -> None:
    cache = MarketCache()
    callbacks = []
    cache.add_callback(callbacks.append)

    class Client:
        calls = 0

        def get_multiple_markets(self, event_ids: object) -> dict[str, object]:
            self.calls += 1
            return {
                "1": [
                    {
                        "id": 10,
                        "selections": [
                            [
                                {
                                    "strike_id": "s",
                                    "name": "home",
                                    "price": 100,
                                    "quantity": 1,
                                }
                            ]
                        ],
                    }
                ]
            }

    client = Client()
    assert cache.seed(client, [1]) is True
    assert cache.seed(client, [1]) is False
    assert client.calls == 1
    assert len(callbacks) == 1
    assert cache.wait_for_update(after_monotonic=0, timeout=0) is True
