from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

from fastprophetx import ProphetXClient, RateLimitPolicy, RequestScheduler


@pytest.fixture
def make_client() -> Iterator[Callable[..., ProphetXClient]]:
    clients: list[ProphetXClient] = []

    def factory(
        handler: Callable[[httpx.Request], httpx.Response],
        **kwargs: Any,
    ) -> ProphetXClient:
        scheduler = kwargs.pop(
            "scheduler",
            RequestScheduler(
                RateLimitPolicy(
                    requests_per_second=1_000_000,
                    market_path_spacing=0,
                    fallback_backoff_base=0,
                    fallback_backoff_cap=0,
                )
            ),
        )
        client = ProphetXClient(
            "public-test-key",
            "private-test-key",
            environment="sandbox",
            transport=httpx.MockTransport(handler),
            scheduler=scheduler,
            **kwargs,
        )
        clients.append(client)
        return client

    yield factory
    for client in clients:
        client.close()
