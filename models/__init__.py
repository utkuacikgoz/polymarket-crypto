"""
Data models for the market data connectors.

This package contains data models organized by source:
- common: Shared enums, base types, and utilities
- binance: Binance-specific models (PriceTick, etc.)
- polymarket_ws: Polymarket WebSocket message models
- polymarket_rest: Polymarket REST API models
"""

from models.common import (
    SourceType,
    MarketStatus,
    Side,
    current_ts_ms,
)

from models.binance import (
    PriceTick,
)

from models.polymarket_ws import (
    # Order book types
    OrderSummary,
    BookMessage,
    # Price change types
    PriceChange,
    PriceChangeMessage,
    # Trade types
    LastTradePriceMessage,
    # BBO types
    BestBidAskMessage,
    # Market lifecycle types
    EventMessage,
    NewMarketMessage,
    MarketResolvedMessage,
    # Tick size types
    TickSizeChangeMessage,
    # Parser
    parse_ws_message,
    PolymarketWSMessage,
)

from models.polymarket_rest import (
    MarketSpec,
    MarketPriceTick,
    MarketSnapshot,
)

from models.health import (
    ConnectorHealth,
    HealthEvent,
)

__all__ = [
    # Common
    "SourceType",
    "MarketStatus",
    "Side",
    "current_ts_ms",
    # Binance
    "PriceTick",
    # Polymarket WS
    "OrderSummary",
    "BookMessage",
    "PriceChange",
    "PriceChangeMessage",
    "LastTradePriceMessage",
    "BestBidAskMessage",
    "EventMessage",
    "NewMarketMessage",
    "MarketResolvedMessage",
    "TickSizeChangeMessage",
    "parse_ws_message",
    "PolymarketWSMessage",
    # Polymarket REST
    "MarketSpec",
    "MarketPriceTick",
    "MarketSnapshot",
    # Health
    "ConnectorHealth",
    "HealthEvent",
]
