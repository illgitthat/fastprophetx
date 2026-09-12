"""Required endpoint presence for the supported ProphetX client surface."""

from __future__ import annotations

from typing import Any

SWAGGER_URL = "https://cash.api.prophetx.co/partner/swagger/mm/doc.json"
REQUIRED_ENDPOINTS = {
    "/auth/login": "post",
    "/auth/refresh": "post",
    "/mm/get_tournaments": "get",
    "/mm/get_sport_events": "get",
    "/v4/mm/get_balance": "get",
    "/v4/mm/get_markets": "get",
    "/v4/mm/get_multiple_markets": "get",
    "/v4/mm/get_price_ladder": "get",
    "/v4/mm/get_strikes": "get",
    "/websocket/connection-config": "get",
    "/v4/mm/websocket": "post",
    "/v4/mm/submit_order": "post",
    "/v4/mm/submit_multiple_orders": "post",
    "/v4/mm/get_order/{id}": "get",
    "/v4/mm/get_order_history": "get",
    "/v4/mm/get_order_matched_detail": "get",
    "/v4/mm/get_trades": "get",
    "/v4/mm/get_transactions": "get",
    "/v4/mm/cancel_order": "post",
    "/v4/mm/cancel_multiple_orders": "post",
    "/v4/mm/cancel_all_orders": "post",
    "/v4/mm/cancel_orders_by_event": "post",
    "/v4/mm/cancel_orders_by_market": "post",
}
_REQUIRED_QUERY_PARAMETERS = {
    ("/v4/mm/get_markets", "get"): {"event_id"},
    ("/v4/mm/get_multiple_markets", "get"): {"event_ids"},
    ("/v4/mm/get_strikes", "get"): {"strike_ids"},
    ("/v4/mm/get_order_history", "get"): {
        "from",
        "to",
        "updated_at_from",
        "updated_at_to",
        "next_cursor",
    },
    ("/v4/mm/get_order_matched_detail", "get"): {
        "order_id",
        "order_ids",
        "from",
        "to",
        "next_cursor",
    },
    ("/v4/mm/get_trades", "get"): {"from", "to", "next_cursor"},
    ("/v4/mm/get_transactions", "get"): {
        "from",
        "to",
        "next_cursor",
        "trade_id",
        "transaction_type",
    },
}
_REQUIRED_BODY_PROPERTIES = {
    ("/auth/login", "post"): {"access_key", "secret_key"},
    ("/auth/refresh", "post"): {"refresh_token"},
    ("/v4/mm/websocket", "post"): {"socket_id", "subscriptions"},
    ("/v4/mm/submit_order", "post"): {
        "external_id",
        "strike_id",
        "price",
        "quantity",
        "order_strategy",
    },
    ("/v4/mm/submit_multiple_orders", "post"): {"data"},
    ("/v4/mm/cancel_order", "post"): {"order_id", "external_id"},
    ("/v4/mm/cancel_multiple_orders", "post"): {"data"},
    ("/v4/mm/cancel_orders_by_event", "post"): {"event_id"},
    ("/v4/mm/cancel_orders_by_market", "post"): {
        "event_id",
        "market_id",
        "strike_id",
    },
}
_SYNCED_RESPONSE_PATHS = {
    ("/v4/mm/get_order/{id}", "get"),
    ("/v4/mm/get_order_history", "get"),
    ("/v4/mm/get_order_matched_detail", "get"),
    ("/v4/mm/get_trades", "get"),
}


def missing_required_endpoints(document: dict[str, Any]) -> set[tuple[str, str]]:
    paths = document.get("paths")
    if not isinstance(paths, dict):
        return {(path, method) for path, method in REQUIRED_ENDPOINTS.items()}
    return {
        (path, method)
        for path, method in REQUIRED_ENDPOINTS.items()
        if not isinstance(paths.get(path), dict) or method not in paths[path]
    }


def contract_violations(document: dict[str, Any]) -> set[str]:
    violations = {
        f"missing operation {method.upper()} {path}"
        for path, method in missing_required_endpoints(document)
    }
    security = document.get("securityDefinitions")
    token = security.get("Token") if isinstance(security, dict) else None
    if not isinstance(token, dict):
        violations.add("missing Token security definition")
    else:
        if token.get("type") != "apiKey":
            violations.add("Token security type is not apiKey")
        if token.get("name") != "Authorization" or token.get("in") != "header":
            violations.add("Token security is not the Authorization header")
        description = token.get("description")
        if not isinstance(description, str) or "Bearer" not in description:
            violations.add("Token security does not document the Bearer prefix")

    for operation_key, names in _REQUIRED_QUERY_PARAMETERS.items():
        operation = _operation(document, *operation_key)
        if operation is None:
            continue
        actual = {
            parameter.get("name")
            for parameter in operation.get("parameters", [])
            if isinstance(parameter, dict) and parameter.get("in") == "query"
        }
        for name in names - actual:
            violations.add(
                f"{operation_key[1].upper()} {operation_key[0]} "
                f"is missing query parameter {name}"
            )

    for operation_key, names in _REQUIRED_BODY_PROPERTIES.items():
        operation = _operation(document, *operation_key)
        if operation is None:
            continue
        schema = _body_schema(document, operation)
        properties = schema.get("properties") if isinstance(schema, dict) else None
        actual = set(properties) if isinstance(properties, dict) else set()
        for name in names - actual:
            violations.add(
                f"{operation_key[1].upper()} {operation_key[0]} "
                f"is missing body property {name}"
            )

    submit_schema = _body_schema(
        document,
        _operation(document, "/v4/mm/submit_order", "post") or {},
    )
    required = (
        submit_schema.get("required") if isinstance(submit_schema, dict) else None
    )
    if isinstance(required, list) and "order_strategy" in required:
        violations.add("submit_order requires order_strategy")

    for path, method in _SYNCED_RESPONSE_PATHS:
        operation = _operation(document, path, method)
        if operation is None:
            continue
        schema = _response_schema(document, operation, "200")
        properties = schema.get("properties") if isinstance(schema, dict) else None
        if not isinstance(properties, dict) or "last_synced_at" not in properties:
            violations.add(f"{method.upper()} {path} is missing last_synced_at")

    config_operation = _operation(
        document,
        "/websocket/connection-config",
        "get",
    )
    if config_operation is not None:
        schema = _response_schema(document, config_operation, "200")
        properties = schema.get("properties") if isinstance(schema, dict) else None
        if not isinstance(properties, dict) or "key" not in properties:
            violations.add("websocket config is missing key")
        if not isinstance(properties, dict) or not {"ws_host", "cluster"} & set(
            properties
        ):
            violations.add("websocket config is missing ws_host and cluster")
    return violations


def _operation(
    document: dict[str, Any],
    path: str,
    method: str,
) -> dict[str, Any] | None:
    paths = document.get("paths")
    path_item = paths.get(path) if isinstance(paths, dict) else None
    operation = path_item.get(method) if isinstance(path_item, dict) else None
    return operation if isinstance(operation, dict) else None


def _body_schema(
    document: dict[str, Any],
    operation: dict[str, Any],
) -> dict[str, Any]:
    parameters = operation.get("parameters")
    if not isinstance(parameters, list):
        return {}
    for parameter in parameters:
        if isinstance(parameter, dict) and parameter.get("in") == "body":
            return _resolve_schema(document, parameter.get("schema"))
    return {}


def _response_schema(
    document: dict[str, Any],
    operation: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    responses = operation.get("responses")
    response = responses.get(status) if isinstance(responses, dict) else None
    return (
        _resolve_schema(document, response.get("schema"))
        if isinstance(response, dict)
        else {}
    )


def _resolve_schema(document: dict[str, Any], schema: Any) -> dict[str, Any]:
    if not isinstance(schema, dict):
        return {}
    reference = schema.get("$ref")
    if not isinstance(reference, str):
        return schema
    prefix = "#/definitions/"
    if not reference.startswith(prefix):
        return {}
    definitions = document.get("definitions")
    resolved = (
        definitions.get(reference.removeprefix(prefix))
        if isinstance(definitions, dict)
        else None
    )
    return resolved if isinstance(resolved, dict) else {}
