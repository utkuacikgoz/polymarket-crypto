"""
Data models for the market data connectors.

This package contains data models organized by source:
- common: Shared enums, base types, and utilities
- binance: Binance-specific models (PriceTick, etc.)
- polymarket_ws: Polymarket WebSocket message models
- polymarket_rest: Polymarket REST API models
"""

from models.binance import (
    PriceTick,
)
from models.common import (
    MarketStatus,
    Side,
    SourceType,
    current_ts_ms,
)
from models.health import (
    ConnectorHealth,
    HealthEvent,
)
from models.polymarket_rest import (
    MarketPriceTick,
    MarketSnapshot,
    MarketSpec,
)
from models.polymarket_ws import (
    # BBO types
    BestBidAskMessage,
    BookMessage,
    # Market lifecycle types
    EventMessage,
    # Trade types
    LastTradePriceMessage,
    MarketResolvedMessage,
    NewMarketMessage,
    # Order book types
    OrderSummary,
    PolymarketWSMessage,
    # Price change types
    PriceChange,
    PriceChangeMessage,
    # Tick size types
    TickSizeChangeMessage,
    # Parser
    parse_ws_message,
)
from models.rtds import (
    ChainlinkPriceTick,
    RTDSSource,
    RTDSSubscription,
    parse_rtds_message,
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
    # RTDS / Chainlink
    "RTDSSource",
    "ChainlinkPriceTick",
    "RTDSSubscription",
    "parse_rtds_message",
    # Health
    "ConnectorHealth",
    "HealthEvent",
]
