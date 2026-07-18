"""
Polymarket WebSocket message models.

Data models for all message types from the Polymarket CLOB WebSocket market channel:
- book: Full order book snapshot
- price_change: Incremental order book updates
- last_trade_price: Trade execution notifications
- best_bid_ask: BBO updates (requires custom_feature_enabled)
- tick_size_change: Tick size change notifications
- new_market: New market created (requires custom_feature_enabled)
- market_resolved: Market resolution (requires custom_feature_enabled)

Reference: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
"""

import json
from dataclasses import dataclass
from typing import Any

from models.common import current_ts_ms

# =============================================================================
# Order Book Types
# =============================================================================

@dataclass(frozen=True)
class OrderSummary:
    """
    A single price level in the order book.

    Attributes:
        price: Price at this level (string to preserve precision)
        size: Total size available at this price level
    """
    price: str
    size: str

    @property
    def price_float(self) -> float:
        """Get price as float."""
        return float(self.price)

    @property
    def size_float(self) -> float:
        """Get size as float."""
        return float(self.size)

    def to_dict(self) -> dict[str, str]:
        """Serialize to dictionary."""
        return {"price": self.price, "size": self.size}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrderSummary":
        """Parse from API response dict."""
        return cls(
            price=str(data.get("price", "0")),
            size=str(data.get("size", "0"))
        )


@dataclass(frozen=True)
class BookMessage:
    """
    Full order book snapshot message.

    Emitted when:
    - First subscribed to a market
    - When there is a trade that affects the book

    Attributes:
        event_type: Always "book"
        asset_id: Token ID
        market: Condition ID of market
        timestamp: Unix timestamp in milliseconds
        hash: Hash summary of the orderbook content
        bids: List of bid levels (buy orders), sorted by price descending
        asks: List of ask levels (sell orders), sorted by price ascending
    """
    event_type: str
    asset_id: str
    market: str
    timestamp: str
    hash: str
    bids: tuple  # Tuple[OrderSummary, ...] for immutability
    asks: tuple  # Tuple[OrderSummary, ...]

    @property
    def ts_ms(self) -> int:
        """Get timestamp as integer milliseconds."""
        try:
            return int(self.timestamp)
        except (ValueError, TypeError):
            return current_ts_ms()

    @property
    def best_bid(self) -> OrderSummary | None:
        """Get the best (highest) bid."""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> OrderSummary | None:
        """Get the best (lowest) ask."""
        return self.asks[0] if self.asks else None

    @property
    def best_bid_price(self) -> float | None:
        """Get the best bid price as float."""
        return self.best_bid.price_float if self.best_bid else None

    @property
    def best_ask_price(self) -> float | None:
        """Get the best ask price as float."""
        return self.best_ask.price_float if self.best_ask else None

    @property
    def mid_price(self) -> float | None:
        """Calculate mid-price from best bid/ask."""
        bid = self.best_bid_price
        ask = self.best_ask_price
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        return bid or ask

    @property
    def spread(self) -> float | None:
        """Calculate bid-ask spread."""
        bid = self.best_bid_price
        ask = self.best_ask_price
        if bid is not None and ask is not None:
            return ask - bid
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "book_message",
            "event_type": self.event_type,
            "asset_id": self.asset_id,
            "market": self.market,
            "timestamp": self.timestamp,
            "hash": self.hash,
            "bids": [b.to_dict() for b in self.bids],
            "asks": [a.to_dict() for a in self.asks],
            "best_bid": self.best_bid_price,
            "best_ask": self.best_ask_price,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BookMessage":
        """
        Parse from WebSocket message dict.

        Expected format:
        {
            "event_type": "book",
            "asset_id": "...",
            "market": "0x...",
            "bids": [{"price": ".48", "size": "30"}, ...],
            "asks": [{"price": ".52", "size": "25"}, ...],
            "timestamp": "123456789000",
            "hash": "0x..."
        }
        """
        bids_raw = data.get("bids", [])
        asks_raw = data.get("asks", [])

        bids = tuple(OrderSummary.from_dict(b) for b in bids_raw)
        asks = tuple(OrderSummary.from_dict(a) for a in asks_raw)

        return cls(
            event_type=data.get("event_type", "book"),
            asset_id=data.get("asset_id", ""),
            market=data.get("market", ""),
            timestamp=str(data.get("timestamp", "")),
            hash=data.get("hash", ""),
            bids=bids,
            asks=asks,
        )


# =============================================================================
# Price Change Types
# =============================================================================

@dataclass(frozen=True)
class PriceChange:
    """
    A single price change in an incremental update.

    Attributes:
        asset_id: Token ID affected
        price: Price level affected
        size: New aggregate size for this price level (0 = level removed)
        side: "BUY" or "SELL"
        hash: Hash of the order
        best_bid: Current best bid price after this change
        best_ask: Current best ask price after this change
    """
    asset_id: str
    price: str
    size: str
    side: str
    hash: str
    best_bid: str
    best_ask: str

    @property
    def price_float(self) -> float:
        """Get price as float."""
        return float(self.price)

    @property
    def size_float(self) -> float:
        """Get size as float."""
        return float(self.size)

    @property
    def best_bid_float(self) -> float | None:
        """Get best bid as float."""
        try:
            return float(self.best_bid) if self.best_bid else None
        except (ValueError, TypeError):
            return None

    @property
    def best_ask_float(self) -> float | None:
        """Get best ask as float."""
        try:
            return float(self.best_ask) if self.best_ask else None
        except (ValueError, TypeError):
            return None

    @property
    def is_bid(self) -> bool:
        """Check if this is a bid (buy) side change."""
        return self.side.upper() == "BUY"

    @property
    def is_ask(self) -> bool:
        """Check if this is an ask (sell) side change."""
        return self.side.upper() == "SELL"

    @property
    def is_removal(self) -> bool:
        """Check if this change removes a price level."""
        return self.size_float == 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "asset_id": self.asset_id,
            "price": self.price,
            "size": self.size,
            "side": self.side,
            "hash": self.hash,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PriceChange":
        """Parse from API response dict."""
        return cls(
            asset_id=data.get("asset_id", ""),
            price=str(data.get("price", "0")),
            size=str(data.get("size", "0")),
            side=data.get("side", ""),
            hash=data.get("hash", ""),
            best_bid=str(data.get("best_bid", "")),
            best_ask=str(data.get("best_ask", "")),
        )


@dataclass(frozen=True)
class PriceChangeMessage:
    """
    Incremental order book update message.

    Emitted when:
    - A new order is placed
    - An order is cancelled

    Attributes:
        event_type: Always "price_change"
        market: Condition ID of market
        timestamp: Unix timestamp in milliseconds
        price_changes: List of individual price changes
    """
    event_type: str
    market: str
    timestamp: str
    price_changes: tuple  # Tuple[PriceChange, ...] for immutability

    @property
    def ts_ms(self) -> int:
        """Get timestamp as integer milliseconds."""
        try:
            return int(self.timestamp)
        except (ValueError, TypeError):
            return current_ts_ms()

    @property
    def affected_assets(self) -> set:
        """Get set of all asset IDs affected by this message."""
        return {pc.asset_id for pc in self.price_changes}

    def get_changes_for_asset(self, asset_id: str) -> list["PriceChange"]:
        """Get all price changes for a specific asset."""
        return [pc for pc in self.price_changes if pc.asset_id == asset_id]

    def get_best_bbo(self, asset_id: str) -> tuple:
        """
        Get the latest best bid/ask for an asset from the changes.

        Returns:
            Tuple of (best_bid, best_ask) as floats, or (None, None)
        """
        for pc in reversed(self.price_changes):
            if pc.asset_id == asset_id:
                return (pc.best_bid_float, pc.best_ask_float)
        return (None, None)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "price_change_message",
            "event_type": self.event_type,
            "market": self.market,
            "timestamp": self.timestamp,
            "price_changes": [pc.to_dict() for pc in self.price_changes],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PriceChangeMessage":
        """
        Parse from WebSocket message dict.

        Expected format:
        {
            "market": "0x...",
            "price_changes": [
                {
                    "asset_id": "...",
                    "price": "0.5",
                    "size": "200",
                    "side": "BUY",
                    "hash": "...",
                    "best_bid": "0.5",
                    "best_ask": "1"
                },
                ...
            ],
            "timestamp": "1757908892351",
            "event_type": "price_change"
        }
        """
        changes_raw = data.get("price_changes", [])
        price_changes = tuple(PriceChange.from_dict(pc) for pc in changes_raw)

        return cls(
            event_type=data.get("event_type", "price_change"),
            market=data.get("market", ""),
            timestamp=str(data.get("timestamp", "")),
            price_changes=price_changes,
        )


# =============================================================================
# Trade Types
# =============================================================================

@dataclass(frozen=True)
class LastTradePriceMessage:
    """
    Last trade price notification.

    Emitted when a maker and taker order is matched creating a trade event.

    Attributes:
        event_type: Always "last_trade_price"
        asset_id: Token ID traded
        market: Condition ID of market
        price: Trade execution price
        size: Trade size
        side: "BUY" or "SELL" (taker side)
        fee_rate_bps: Fee rate in basis points
        timestamp: Unix timestamp in milliseconds
    """
    event_type: str
    asset_id: str
    market: str
    price: str
    size: str
    side: str
    fee_rate_bps: str
    timestamp: str

    @property
    def ts_ms(self) -> int:
        """Get timestamp as integer milliseconds."""
        try:
            return int(self.timestamp)
        except (ValueError, TypeError):
            return current_ts_ms()

    @property
    def price_float(self) -> float:
        """Get price as float."""
        return float(self.price)

    @property
    def size_float(self) -> float:
        """Get size as float."""
        return float(self.size)

    @property
    def fee_bps(self) -> int:
        """Get fee rate in basis points."""
        try:
            return int(self.fee_rate_bps)
        except (ValueError, TypeError):
            return 0

    @property
    def is_buy(self) -> bool:
        """Check if taker was buying."""
        return self.side.upper() == "BUY"

    @property
    def notional(self) -> float:
        """Calculate notional value of trade."""
        return self.price_float * self.size_float

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "last_trade_price_message",
            "event_type": self.event_type,
            "asset_id": self.asset_id,
            "market": self.market,
            "price": self.price,
            "size": self.size,
            "side": self.side,
            "fee_rate_bps": self.fee_rate_bps,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LastTradePriceMessage":
        """
        Parse from WebSocket message dict.

        Expected format:
        {
            "asset_id": "...",
            "event_type": "last_trade_price",
            "fee_rate_bps": "0",
            "market": "0x...",
            "price": "0.456",
            "side": "BUY",
            "size": "219.217767",
            "timestamp": "1750428146322"
        }
        """
        return cls(
            event_type=data.get("event_type", "last_trade_price"),
            asset_id=data.get("asset_id", ""),
            market=data.get("market", ""),
            price=str(data.get("price", "0")),
            size=str(data.get("size", "0")),
            side=data.get("side", ""),
            fee_rate_bps=str(data.get("fee_rate_bps", "0")),
            timestamp=str(data.get("timestamp", "")),
        )


# =============================================================================
# Best Bid/Ask Types
# =============================================================================

@dataclass(frozen=True)
class BestBidAskMessage:
    """
    Best bid/ask update message.

    Emitted when the best bid and ask prices for a market change.
    Note: This message is behind the `custom_feature_enabled` flag.

    Attributes:
        event_type: Always "best_bid_ask"
        market: Condition ID of market
        asset_id: Token ID
        best_bid: Current best bid price
        best_ask: Current best ask price
        spread: Spread between best bid and ask
        timestamp: Unix timestamp in milliseconds
    """
    event_type: str
    market: str
    asset_id: str
    best_bid: str
    best_ask: str
    spread: str
    timestamp: str

    @property
    def ts_ms(self) -> int:
        """Get timestamp as integer milliseconds."""
        try:
            return int(self.timestamp)
        except (ValueError, TypeError):
            return current_ts_ms()

    @property
    def best_bid_float(self) -> float:
        """Get best bid as float."""
        return float(self.best_bid)

    @property
    def best_ask_float(self) -> float:
        """Get best ask as float."""
        return float(self.best_ask)

    @property
    def spread_float(self) -> float:
        """Get spread as float."""
        return float(self.spread)

    @property
    def mid_price(self) -> float:
        """Calculate mid-price."""
        return (self.best_bid_float + self.best_ask_float) / 2

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "best_bid_ask_message",
            "event_type": self.event_type,
            "market": self.market,
            "asset_id": self.asset_id,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread": self.spread,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BestBidAskMessage":
        """
        Parse from WebSocket message dict.

        Expected format:
        {
            "event_type": "best_bid_ask",
            "market": "0x...",
            "asset_id": "...",
            "best_bid": "0.73",
            "best_ask": "0.77",
            "spread": "0.04",
            "timestamp": "1766789469958"
        }
        """
        return cls(
            event_type=data.get("event_type", "best_bid_ask"),
            market=data.get("market", ""),
            asset_id=data.get("asset_id", ""),
            best_bid=str(data.get("best_bid", "0")),
            best_ask=str(data.get("best_ask", "0")),
            spread=str(data.get("spread", "0")),
            timestamp=str(data.get("timestamp", "")),
        )


# =============================================================================
# Tick Size Types
# =============================================================================

@dataclass(frozen=True)
class TickSizeChangeMessage:
    """
    Tick size change notification.

    Emitted when the minimum tick size of the market changes.
    This happens when the book's price reaches the limits: price > 0.96 or price < 0.04

    Attributes:
        event_type: Always "tick_size_change"
        asset_id: Token ID
        market: Condition ID of market
        old_tick_size: Previous minimum tick size
        new_tick_size: Current minimum tick size
        side: buy/sell
        timestamp: Unix timestamp in milliseconds
    """
    event_type: str
    asset_id: str
    market: str
    old_tick_size: str
    new_tick_size: str
    side: str
    timestamp: str

    @property
    def ts_ms(self) -> int:
        """Get timestamp as integer milliseconds."""
        try:
            return int(self.timestamp)
        except (ValueError, TypeError):
            return current_ts_ms()

    @property
    def old_tick_size_float(self) -> float:
        """Get old tick size as float."""
        return float(self.old_tick_size)

    @property
    def new_tick_size_float(self) -> float:
        """Get new tick size as float."""
        return float(self.new_tick_size)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "tick_size_change_message",
            "event_type": self.event_type,
            "asset_id": self.asset_id,
            "market": self.market,
            "old_tick_size": self.old_tick_size,
            "new_tick_size": self.new_tick_size,
            "side": self.side,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TickSizeChangeMessage":
        """
        Parse from WebSocket message dict.

        Expected format:
        {
            "event_type": "tick_size_change",
            "asset_id": "...",
            "market": "0x...",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": "100000000"
        }
        """
        return cls(
            event_type=data.get("event_type", "tick_size_change"),
            asset_id=data.get("asset_id", ""),
            market=data.get("market", ""),
            old_tick_size=str(data.get("old_tick_size", "0")),
            new_tick_size=str(data.get("new_tick_size", "0")),
            side=data.get("side", ""),
            timestamp=str(data.get("timestamp", "")),
        )


# =============================================================================
# Market Lifecycle Types
# =============================================================================

@dataclass(frozen=True)
class EventMessage:
    """
    Event metadata included in new_market and market_resolved messages.

    Attributes:
        id: Event message ID
        ticker: Event message ticker
        slug: Event message slug
        title: Event message title
        description: Event message description
    """
    id: str
    ticker: str
    slug: str
    title: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "id": self.id,
            "ticker": self.ticker,
            "slug": self.slug,
            "title": self.title,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventMessage":
        """Parse from API response dict."""
        return cls(
            id=data.get("id", ""),
            ticker=data.get("ticker", ""),
            slug=data.get("slug", ""),
            title=data.get("title", ""),
            description=data.get("description", ""),
        )


@dataclass(frozen=True)
class NewMarketMessage:
    """
    New market creation notification.

    Emitted when a new market is created.
    Note: This message is behind the `custom_feature_enabled` flag.

    Attributes:
        event_type: Always "new_market"
        id: Market ID
        question: Market question
        market: Condition ID of market
        slug: Market slug
        description: Market description
        assets_ids: List of asset IDs (token IDs)
        outcomes: List of outcomes (e.g., ["Yes", "No"])
        event_message: Event metadata
        timestamp: Unix timestamp in milliseconds
    """
    event_type: str
    id: str
    question: str
    market: str
    slug: str
    description: str
    assets_ids: tuple  # Tuple[str, ...] for immutability
    outcomes: tuple  # Tuple[str, ...] for immutability
    event_message: EventMessage | None
    timestamp: str

    @property
    def ts_ms(self) -> int:
        """Get timestamp as integer milliseconds."""
        try:
            return int(self.timestamp)
        except (ValueError, TypeError):
            return current_ts_ms()

    @property
    def token_yes(self) -> str | None:
        """Get the YES token ID (first asset)."""
        return self.assets_ids[0] if self.assets_ids else None

    @property
    def token_no(self) -> str | None:
        """Get the NO token ID (second asset)."""
        return self.assets_ids[1] if len(self.assets_ids) > 1 else None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "new_market_message",
            "event_type": self.event_type,
            "id": self.id,
            "question": self.question,
            "market": self.market,
            "slug": self.slug,
            "description": self.description[:200] + "..." if len(self.description) > 200 else self.description,
            "assets_ids": list(self.assets_ids),
            "outcomes": list(self.outcomes),
            "event_message": self.event_message.to_dict() if self.event_message else None,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NewMarketMessage":
        """
        Parse from WebSocket message dict.
        """
        event_msg_raw = data.get("event_message")
        event_message = EventMessage.from_dict(event_msg_raw) if event_msg_raw else None

        return cls(
            event_type=data.get("event_type", "new_market"),
            id=data.get("id", ""),
            question=data.get("question", ""),
            market=data.get("market", ""),
            slug=data.get("slug", ""),
            description=data.get("description", ""),
            assets_ids=tuple(data.get("assets_ids", [])),
            outcomes=tuple(data.get("outcomes", [])),
            event_message=event_message,
            timestamp=str(data.get("timestamp", "")),
        )


@dataclass(frozen=True)
class MarketResolvedMessage:
    """
    Market resolution notification.

    Emitted when a market is resolved.
    Note: This message is behind the `custom_feature_enabled` flag.

    Attributes:
        event_type: Always "market_resolved"
        id: Market ID
        question: Market question
        market: Condition ID of market
        slug: Market slug
        description: Market description
        assets_ids: List of asset IDs
        outcomes: List of outcomes
        winning_asset_id: Token ID of the winning outcome
        winning_outcome: Name of the winning outcome
        event_message: Event metadata
        timestamp: Unix timestamp in milliseconds
    """
    event_type: str
    id: str
    question: str
    market: str
    slug: str
    description: str
    assets_ids: tuple
    outcomes: tuple
    winning_asset_id: str
    winning_outcome: str
    event_message: EventMessage | None
    timestamp: str

    @property
    def ts_ms(self) -> int:
        """Get timestamp as integer milliseconds."""
        try:
            return int(self.timestamp)
        except (ValueError, TypeError):
            return current_ts_ms()

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "market_resolved_message",
            "event_type": self.event_type,
            "id": self.id,
            "question": self.question,
            "market": self.market,
            "slug": self.slug,
            "winning_asset_id": self.winning_asset_id,
            "winning_outcome": self.winning_outcome,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MarketResolvedMessage":
        """Parse from WebSocket message dict."""
        event_msg_raw = data.get("event_message")
        event_message = EventMessage.from_dict(event_msg_raw) if event_msg_raw else None

        return cls(
            event_type=data.get("event_type", "market_resolved"),
            id=data.get("id", ""),
            question=data.get("question", ""),
            market=data.get("market", ""),
            slug=data.get("slug", ""),
            description=data.get("description", ""),
            assets_ids=tuple(data.get("assets_ids", [])),
            outcomes=tuple(data.get("outcomes", [])),
            winning_asset_id=data.get("winning_asset_id", ""),
            winning_outcome=data.get("winning_outcome", ""),
            event_message=event_message,
            timestamp=str(data.get("timestamp", "")),
        )


# =============================================================================
# Message Type Union and Parser
# =============================================================================

# Union type for all possible WebSocket messages
PolymarketWSMessage = (
    BookMessage
    | PriceChangeMessage
    | LastTradePriceMessage
    | BestBidAskMessage
    | TickSizeChangeMessage
    | NewMarketMessage
    | MarketResolvedMessage
)

# Mapping of event_type to parser class
_MESSAGE_PARSERS: dict[str, type] = {
    "book": BookMessage,
    "price_change": PriceChangeMessage,
    "last_trade_price": LastTradePriceMessage,
    "best_bid_ask": BestBidAskMessage,
    "tick_size_change": TickSizeChangeMessage,
    "new_market": NewMarketMessage,
    "market_resolved": MarketResolvedMessage,
}


def parse_ws_message(data: dict[str, Any]) -> PolymarketWSMessage | None:
    """
    Parse a WebSocket message into the appropriate typed model.

    Args:
        data: Raw message dictionary from WebSocket

    Returns:
        Typed message object, or None if unknown/invalid message type

    Example:
        >>> msg = parse_ws_message({"event_type": "book", "asset_id": "...", ...})
        >>> if isinstance(msg, BookMessage):
        ...     print(f"Best bid: {msg.best_bid_price}")
    """
    event_type = data.get("event_type", data.get("type", ""))

    parser_class = _MESSAGE_PARSERS.get(event_type)
    if parser_class is None:
        return None

    try:
        return parser_class.from_dict(data)
    except Exception:
        return None


def parse_ws_messages(raw: str) -> list[PolymarketWSMessage]:
    """
    Parse a raw WebSocket message string (may be single message or batch).

    Args:
        raw: Raw JSON string from WebSocket

    Returns:
        List of parsed message objects (empty if parse error)
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []

    # Handle batch messages (array)
    if isinstance(data, list):
        messages = []
        for item in data:
            msg = parse_ws_message(item)
            if msg is not None:
                messages.append(msg)
        return messages

    # Handle single message
    if isinstance(data, dict):
        msg = parse_ws_message(data)
        return [msg] if msg is not None else []

    return []
