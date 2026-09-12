"""Read-only live smoke test with an explicitly supplied environment file."""

from __future__ import annotations

import argparse
from pathlib import Path

from fastprophetx import MarketStore, ProphetXClient, ProphetXWebSocket


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        if not name or not name.replace("_", "").isalnum():
            raise ValueError(f"invalid variable name at line {line_number}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[name] = value
    return values


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment", required=True, choices=("sandbox", "production")
    )
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--websocket-timeout", type=float, default=15.0)
    args = parser.parse_args()
    values = load_env_file(args.env_file)

    with ProphetXClient.from_env(args.environment, environ=values) as client:
        client.authenticate()
        balance = client.get_balance()
        tournaments = client.get_tournaments()
        print(
            f"REST ready: environment={args.environment} "
            f"balance_fields={sorted(balance)} tournaments={len(tournaments)}"
        )
        event_ids: list[int] = []
        tournament_id = None
        events: list[dict[str, object]] = []
        store = MarketStore(client)
        for tournament in tournaments[:10]:
            candidate_id = tournament.get("id")
            if not isinstance(candidate_id, int):
                continue
            candidate_events = client.get_sport_events(candidate_id)
            candidate_event_ids = [
                event_id
                for event in candidate_events
                if isinstance((event_id := event.get("event_id")), int)
            ][:2]
            if candidate_event_ids and any(store.refresh(candidate_event_ids).values()):
                tournament_id = candidate_id
                events = candidate_events
                event_ids = candidate_event_ids
                break
        if not event_ids:
            print("No event with materializable markets was available")
            return
        print(f"Events ready: tournament_id={tournament_id} events={len(events)}")
        stream = ProphetXWebSocket(client, cache=store.cache)
        try:
            stream.start(event_ids=event_ids)
            if not stream.wait_ready(args.websocket_timeout):
                raise RuntimeError(
                    stream.cache.last_error or "WebSocket readiness timed out"
                )
            print(
                f"WebSocket ready: event_ids={event_ids} "
                f"channels={len(stream.cache.authorized_scopes)}"
            )
            markets = store.refresh(event_ids)
            print(
                "Books valid: "
                f"events={sum(bool(items) for items in markets.values())} "
                f"selections={len(stream.cache.snapshots())}"
            )
        finally:
            stream.close()


if __name__ == "__main__":
    main()
