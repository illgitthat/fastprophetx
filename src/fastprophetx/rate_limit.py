"""Thread-safe ProphetX request scheduling and cooldowns."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class RateLimitPolicy:
    __slots__ = (
        "fallback_backoff_base",
        "fallback_backoff_cap",
        "market_path_spacing",
        "max_429_retries",
        "requests_per_second",
    )

    def __init__(
        self,
        *,
        requests_per_second: float = 50.0,
        market_path_spacing: float = 1.05,
        max_429_retries: int = 3,
        fallback_backoff_base: float = 0.25,
        fallback_backoff_cap: float = 8.0,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        if market_path_spacing < 0:
            raise ValueError("market_path_spacing cannot be negative")
        if max_429_retries < 0:
            raise ValueError("max_429_retries cannot be negative")
        if fallback_backoff_base < 0 or fallback_backoff_cap < 0:
            raise ValueError("fallback backoff values cannot be negative")
        self.requests_per_second = requests_per_second
        self.market_path_spacing = market_path_spacing
        self.max_429_retries = max_429_retries
        self.fallback_backoff_base = fallback_backoff_base
        self.fallback_backoff_cap = fallback_backoff_cap


class RequestScheduler:
    def __init__(
        self,
        policy: RateLimitPolicy | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.policy = policy or RateLimitPolicy()
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._last_request = float("-inf")
        self._last_market_path: dict[str, float] = {}
        self._cooldown_until: dict[str, float] = {}
        self._global_cooldown_until = 0.0
        self._requests = 0
        self._cooldowns = 0
        self._wait_seconds = 0.0

    def acquire(self, path: str, *, market_query: bool = False) -> None:
        while True:
            with self._lock:
                now = self._clock()
                ready = max(
                    self._last_request + 1.0 / self.policy.requests_per_second,
                    self._global_cooldown_until,
                    self._cooldown_until.get(path, 0.0),
                )
                if market_query:
                    ready = max(
                        ready,
                        self._last_market_path.get(path, float("-inf"))
                        + self.policy.market_path_spacing,
                    )
                wait = ready - now
                if wait <= 0:
                    self._last_request = now
                    if market_query:
                        self._last_market_path[path] = now
                    self._requests += 1
                    return
                self._wait_seconds += wait
            self._sleep(wait)

    def cooldown(self, path: str, seconds: float, *, global_: bool = False) -> None:
        seconds = max(0.0, seconds)
        with self._lock:
            deadline = self._clock() + seconds
            if global_:
                self._global_cooldown_until = max(self._global_cooldown_until, deadline)
            else:
                self._cooldown_until[path] = max(
                    self._cooldown_until.get(path, 0.0), deadline
                )
            self._cooldowns += 1

    def diagnostics(self) -> dict[str, object]:
        with self._lock:
            now = self._clock()
            return {
                "requests": self._requests,
                "cooldowns": self._cooldowns,
                "wait_seconds": self._wait_seconds,
                "global_cooldown_remaining": max(
                    0.0, self._global_cooldown_until - now
                ),
                "path_cooldowns": {
                    path: max(0.0, deadline - now)
                    for path, deadline in self._cooldown_until.items()
                    if deadline > now
                },
            }
