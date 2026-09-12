# fastprophetx

`fastprophetx` is an unofficial Python client for the ProphetX Trading API. It
provides synchronous REST access, live market state, private order updates, and
safe reconciliation primitives.

The package exists to keep exchange mechanics out of trading strategies. It
centralizes authentication, HTTP connections, rate limits, WebSocket lifecycle,
market-cache validity, and uncertain-order handling.

ProphetX owns the API contract and trading semantics. Use its official
documentation for endpoint fields, order states, and exchange rules:

- [ProphetX API documentation](https://docs.prophetx.co/)
- [Trading API integration](https://docs.prophetx.co/docs/integration)
- [WebSocket events](https://docs.prophetx.co/docs/websocket-events)
- [WebSocket use cases](https://docs.prophetx.co/docs/various-websocket-use-cases)

## What it provides

- Sandbox and production clients
- Reusable HTTP/2 connections and automatic token refresh
- Read-only retries and shared rate-limit scheduling
- One WebSocket for event, sub-market, and private order updates
- Sequence-aware market depth with reconnect invalidation
- Focused order submission, reconciliation, fill-detail, and cancellation calls
- Typed errors and small order and market value objects
- American-odds conversion helpers

ProphetX response payloads remain ordinary dictionaries. The package does not
duplicate the complete ProphetX schema as Python models.

## Installation

Python 3.14 or newer is required.

```bash
uv add "fastprophetx @ git+https://github.com/illgitthat/fastprophetx.git"
```

## Quick start

Set credentials for the selected environment:

| Environment | Variables |
|---|---|
| Production | `PROPHETX_ACCESS_KEY`, `PROPHETX_SECRET_KEY` |
| Sandbox | `PROPHETX_SANDBOX_ACCESS_KEY`, `PROPHETX_SANDBOX_SECRET_KEY` |

`fastprophetx` does not load `.env` files. Load application configuration before
creating the client.

```python
from fastprophetx import Environment, ProphetXClient

with ProphetXClient.from_env(Environment.SANDBOX) as client:
    client.authenticate()

    tournaments = client.get_tournaments(active_only=True)
    events = client.get_sport_events(tournament_id=109)
    event_ids = [event["event_id"] for event in events[:2]]
    markets = client.get_multiple_markets(event_ids)
```

Use `ProphetXClient.from_credentials()` when credentials come from a secret
manager instead of environment variables.

## Live market state

`MarketStore` combines authoritative REST structure with WebSocket depth.
`ProphetXWebSocket` invalidates affected books after disconnects and
subscription changes. Reconcile through `MarketStore` before treating them as
current again.

```python
from fastprophetx import MarketStore, ProphetXWebSocket

store = MarketStore(client)
stream = ProphetXWebSocket(client, cache=store.cache)

try:
    stream.start(event_ids)
    if not stream.wait_ready(15):
        raise RuntimeError(stream.cache.last_error or "WebSocket was not ready")

    markets = store.refresh(event_ids)
    if not stream.books_ready(event_ids):
        raise RuntimeError("Market books were not ready")
finally:
    stream.close()
```

Pass exact `(event_id, subtype)` pairs to subscribe to player-prop channels.

## Private order updates

ProphetX includes authenticated order messages in the same registered WebSocket
connection. `on_order_update` exposes their decoded payload and timing metadata
without requiring a second socket.

Keep the callback nonblocking because it runs on the WebSocket receive thread:

```python
from queue import SimpleQueue

order_updates = SimpleQueue()

stream = ProphetXWebSocket(
    client,
    cache=store.cache,
    on_order_update=lambda payload, exchange_timestamp, received_at, generation: (
        order_updates.put(
            (payload, exchange_timestamp, received_at, generation)
        )
    ),
)
```

See ProphetX's
[WebSocket event reference](https://docs.prophetx.co/docs/websocket-events) for
the order payload and lifecycle fields.

## Orders and reconciliation

Every order needs a unique external ID. A successful submission response means
that ProphetX accepted the request; it does not prove that the order filled.

```python
from fastprophetx import OrderIntent

result = client.submit_order(
    OrderIntent(
        external_id="my-unique-order-id",
        strike_id="prophetx-strike-id",
        price=-110,
        quantity=10.0,
        order_strategy="fillOrKill",
    )
)
```

Mutating requests are never automatically retried after an ambiguous transport
or server failure. Check `ProphetXAPIError.outcome_unknown` and reconcile with
the original external ID before submitting a replacement:

```python
order = client.find_order_by_external_id(
    "my-unique-order-id",
    from_timestamp=submitted_at_unix,
)
```

The client also provides focused methods for order lookup, matched details,
trade and transaction history, and single, batch, event, market, or global
cancellation. Refer to the official ProphetX API documentation for their
request and response contracts.

## Operational behavior

- Environments and base URLs are explicit.
- Access tokens refresh before expiry and once after an authenticated 401.
- Eligible read-only requests use bounded retries and server-directed cooldowns.
- Mutations return uncertainty errors instead of success-shaped fallbacks.
- WebSocket reconnects invalidate affected market books.
- Callbacks include local monotonic receipt time and connection generation.
- `client.scheduler.diagnostics()` reports local waits and cooldowns.

## Read-only live check

The smoke script verifies credentials, market reads, and WebSocket readiness. It
does not submit or cancel orders.

```bash
uv run python scripts/live_smoke.py \
  --environment sandbox \
  --env-file /path/to/prophetx.env
```

## Development

```bash
uv sync
uv run pytest
uv run pytest -m schema
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv build
```

The optional schema check compares the required surface with the current
ProphetX Swagger document. Local fixtures cover wire-format behavior.

## Status

`fastprophetx` is alpha software and intentionally covers a focused Trading API
surface. Validate order reconciliation in sandbox before unattended use.

## License

MIT
