"""Fast dictionary-first ProphetX API client."""

from .cache import EventDepthSnapshot, MarketCache, MarketKey, ReconcileRequest
from .client import (
    BASE_URLS,
    PRODUCTION_BASE_URL,
    PROPHETX_PRODUCTION_BASE_URL,
    PROPHETX_SANDBOX_BASE_URL,
    SANDBOX_BASE_URL,
    Environment,
    ProphetXClient,
)
from .errors import (
    AuthenticationError,
    PreSubmitError,
    ProphetXAPIError,
    ProphetXAuthenticationError,
    ProphetXPreSubmitError,
    ProphetXResponseError,
    ProphetXTransportError,
    ResponseError,
    TransportError,
)
from .market_store import MarketStore
from .models import (
    DepthLevel,
    MarketSnapshot,
    OrderIntent,
    OrderResult,
    ProphetXOrderIntent,
    ProphetXOrderResult,
)
from .odds import (
    implied_probability,
    normalize_selection_levels,
    normalized_cost,
    profit_to_stake,
    stake_to_profit,
    validate_american_odds,
)
from .rate_limit import RateLimitPolicy, RequestScheduler
from .websocket import ProphetXWebSocket

ProphetXAPI = ProphetXClient

__all__ = [
    "BASE_URLS",
    "PRODUCTION_BASE_URL",
    "PROPHETX_PRODUCTION_BASE_URL",
    "PROPHETX_SANDBOX_BASE_URL",
    "SANDBOX_BASE_URL",
    "AuthenticationError",
    "DepthLevel",
    "Environment",
    "EventDepthSnapshot",
    "MarketCache",
    "MarketKey",
    "MarketSnapshot",
    "MarketStore",
    "OrderIntent",
    "OrderResult",
    "PreSubmitError",
    "ProphetXAPI",
    "ProphetXAPIError",
    "ProphetXAuthenticationError",
    "ProphetXClient",
    "ProphetXOrderIntent",
    "ProphetXOrderResult",
    "ProphetXPreSubmitError",
    "ProphetXResponseError",
    "ProphetXTransportError",
    "ProphetXWebSocket",
    "RateLimitPolicy",
    "ReconcileRequest",
    "RequestScheduler",
    "ResponseError",
    "TransportError",
    "implied_probability",
    "normalize_selection_levels",
    "normalized_cost",
    "profit_to_stake",
    "stake_to_profit",
    "validate_american_odds",
]
