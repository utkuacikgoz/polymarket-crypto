"""
Polymarket REST API data models.

Models for Polymarket Gamma API and CLOB REST API responses.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Optional

from models.common import MarketStatus, Side, SourceType


@dataclass(frozen=True)
class MarketSpec:
    """
    Specification of a Polymarket market.

    Typically populated from Gamma API responses.

    Attributes:
        market_id: Unique market identifier (condition ID in Polymarket)
        event_id: Parent event ID
        slug: URL-friendly market identifier
        title: Human-readable market title/question
        expiry_ts_ms: Market expiration timestamp in milliseconds
        strike: Strike price for price-based markets (if applicable)
        token_yes: Token ID for the "Yes" outcome
        token_no: Token ID for the "No" outcome
        status: Current market status
        is_active: Whether the market is currently active
        accepts_orders: Whether the market accepts orders
        outcomes: List of outcome names (typically ["Yes", "No"])
        extra: Additional metadata (series_key, etc.)
    """
    market_id: str
    event_id: str
    slug: str
    title: str
    expiry_ts_ms: int
    strike: float | None
    token_yes: str
    token_no: str
    status: MarketStatus
    is_active: bool = True
    accepts_orders: bool = True
    outcomes: tuple = field(default=("Yes", "No"))
    extra: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        result = {
            "type": "market_spec",
            "market_id": self.market_id,
            "event_id": self.event_id,
            "slug": self.slug,
            "title": self.title,
            "expiry_ts_ms": self.expiry_ts_ms,
            "strike": self.strike,
            "token_yes": self.token_yes,
            "token_no": self.token_no,
            "status": self.status.value,
            "is_active": self.is_active,
            "accepts_orders": self.accepts_orders,
            "outcomes": list(self.outcomes)
        }
        if self.extra:
            result["extra"] = self.extra
        return result

    def get_token_ids(self) -> tuple:
        """Return tuple of (token_yes, token_no) for subscription."""
        return (self.token_yes, self.token_no)

    def __eq__(self, other: object) -> bool:
        """Check equality based on market_id only for deduplication."""
        if not isinstance(other, MarketSpec):
            return False
        return self.market_id == other.market_id

    def __hash__(self) -> int:
        """Hash based on market_id for set operations."""
        return hash(self.market_id)

    @classmethod
    def from_gamma_response(cls, data: dict[str, Any]) -> Optional["MarketSpec"]:
        """
        Parse MarketSpec from Gamma API market response.

        Returns None if required fields are missing.
        """
        try:
            # Parse token IDs from clobTokenIds field
            # Can be JSON array string like '["token1", "token2"]' or comma-separated
            clob_token_ids = data.get("clobTokenIds", "")

            tokens = []
            if clob_token_ids:
                # Try JSON array format first
                if clob_token_ids.startswith("["):
                    try:
                        tokens = json.loads(clob_token_ids)
                    except json.JSONDecodeError:
                        pass

                # Fall back to comma-separated
                if not tokens:
                    tokens = [t.strip().strip('"\'') for t in clob_token_ids.split(",") if t.strip()]

            if len(tokens) < 2:
                return None

            token_yes = tokens[0]
            token_no = tokens[1]

            # Parse expiry timestamp
            end_date = data.get("endDate")
            if end_date:
                from datetime import datetime
                try:
                    # Try ISO format
                    dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    expiry_ts_ms = int(dt.timestamp() * 1000)
                except (ValueError, AttributeError):
                    expiry_ts_ms = 0
            else:
                expiry_ts_ms = 0

            # Determine status
            is_closed = data.get("closed", False)
            is_active = data.get("active", False)
            accepts_orders = data.get("acceptingOrders", False)

            if is_closed:
                status = MarketStatus.CLOSED
            elif is_active:
                status = MarketStatus.ACTIVE
            else:
                status = MarketStatus.UNKNOWN

            # Parse outcomes
            outcomes_str = data.get("outcomes", '["Yes", "No"]')
            try:
                outcomes = tuple(json.loads(outcomes_str))
            except (json.JSONDecodeError, TypeError):
                outcomes = ("Yes", "No")

            # Parse strike from groupItemThreshold if present
            strike_str = data.get("groupItemThreshold")
            strike = float(strike_str) if strike_str else None

            # Get event ID from nested events or fallback
            events = data.get("events", [])
            event_id = events[0]["id"] if events else data.get("id", "")

            return cls(
                market_id=data.get("conditionId", data.get("id", "")),
                event_id=str(event_id),
                slug=data.get("slug", ""),
                title=data.get("question", ""),
                expiry_ts_ms=expiry_ts_ms,
                strike=strike,
                token_yes=token_yes,
                token_no=token_no,
                status=status,
                is_active=is_active,
                accepts_orders=accepts_orders,
                outcomes=outcomes
            )
        except (KeyError, ValueError, IndexError):
            return None


@dataclass(frozen=True)
class MarketPriceTick:
    """
    A single price tick from Polymarket for a specific token.

    Used for both WebSocket and REST price updates.

    Attributes:
        ts_ms: Timestamp in milliseconds (epoch)
        market_id: Market condition ID
        token_id: Token ID (yes or no token)
        price: Current price (0-1 range, representing probability)
        side: Optional side indicator
        source: Source of the price data
    """
    ts_ms: int
    market_id: str
    token_id: str
    price: float
    side: Side | None
    source: SourceType

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "market_price_tick",
            "ts_ms": self.ts_ms,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "price": self.price,
            "side": self.side.value if self.side else None,
            "source": self.source.value
        }

    @property
    def is_yes_token(self) -> bool:
        """Check if this is likely a YES token (price > 0.5 typically)."""
        return self.price > 0.5

    @property
    def implied_probability(self) -> float:
        """Get implied probability (same as price for Polymarket)."""
        return self.price


@dataclass(frozen=True)
class MarketSnapshot:
    """
    A complete snapshot of a Polymarket market's current state.

    Combines data from both YES and NO tokens for a market.

    Attributes:
        ts_ms: Timestamp in milliseconds (epoch)
        market_spec: The market specification
        best_yes_price: Best price for the Yes token (0-1)
        best_no_price: Best price for the No token (0-1)
        best_yes_bid: Best bid for Yes token
        best_yes_ask: Best ask for Yes token
        best_no_bid: Best bid for No token
        best_no_ask: Best ask for No token
        source: Source of the snapshot data
    """
    ts_ms: int
    market_spec: MarketSpec
    best_yes_price: float | None
    best_no_price: float | None
    best_yes_bid: float | None = None
    best_yes_ask: float | None = None
    best_no_bid: float | None = None
    best_no_ask: float | None = None
    source: SourceType = SourceType.POLYMARKET_WS

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "market_snapshot",
            "ts_ms": self.ts_ms,
            "market_id": self.market_spec.market_id,
            "best_yes_price": self.best_yes_price,
            "best_no_price": self.best_no_price,
            "best_yes_bid": self.best_yes_bid,
            "best_yes_ask": self.best_yes_ask,
            "best_no_bid": self.best_no_bid,
            "best_no_ask": self.best_no_ask,
            "source": self.source.value
        }

    @property
    def yes_mid(self) -> float | None:
        """Calculate YES token mid-price."""
        if self.best_yes_bid is not None and self.best_yes_ask is not None:
            return (self.best_yes_bid + self.best_yes_ask) / 2
        return self.best_yes_price

    @property
    def no_mid(self) -> float | None:
        """Calculate NO token mid-price."""
        if self.best_no_bid is not None and self.best_no_ask is not None:
            return (self.best_no_bid + self.best_no_ask) / 2
        return self.best_no_price

    @property
    def yes_spread(self) -> float | None:
        """Calculate YES token bid-ask spread."""
        if self.best_yes_bid is not None and self.best_yes_ask is not None:
            return self.best_yes_ask - self.best_yes_bid
        return None

    @property
    def no_spread(self) -> float | None:
        """Calculate NO token bid-ask spread."""
        if self.best_no_bid is not None and self.best_no_ask is not None:
            return self.best_no_ask - self.best_no_bid
        return None


@dataclass(frozen=True)
class OrderBookLevel:
    """
    A single level in the order book from REST API.

    Attributes:
        price: Price at this level
        size: Size available
    """
    price: float
    size: float

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {"price": self.price, "size": self.size}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrderBookLevel":
        """Parse from API response."""
        return cls(
            price=float(data.get("price", 0)),
            size=float(data.get("size", 0))
        )


@dataclass(frozen=True)
class OrderBook:
    """
    Full order book from CLOB REST API.

    Attributes:
        token_id: Token ID this book is for
        market_id: Market condition ID
        bids: List of bid levels (sorted by price descending)
        asks: List of ask levels (sorted by price ascending)
        ts_ms: Timestamp when fetched
    """
    token_id: str
    market_id: str
    bids: tuple  # Tuple[OrderBookLevel, ...]
    asks: tuple  # Tuple[OrderBookLevel, ...]
    ts_ms: int

    @property
    def best_bid(self) -> OrderBookLevel | None:
        """Get best bid level."""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> OrderBookLevel | None:
        """Get best ask level."""
        return self.asks[0] if self.asks else None

    @property
    def best_bid_price(self) -> float | None:
        """Get best bid price."""
        return self.best_bid.price if self.best_bid else None

    @property
    def best_ask_price(self) -> float | None:
        """Get best ask price."""
        return self.best_ask.price if self.best_ask else None

    @property
    def mid_price(self) -> float | None:
        """Calculate mid-price."""
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
            "type": "order_book",
            "token_id": self.token_id,
            "market_id": self.market_id,
            "best_bid": self.best_bid_price,
            "best_ask": self.best_ask_price,
            "mid_price": self.mid_price,
            "spread": self.spread,
            "bid_depth": len(self.bids),
            "ask_depth": len(self.asks),
            "ts_ms": self.ts_ms,
        }

    @classmethod
    def from_rest_response(
        cls,
        data: dict[str, Any],
        token_id: str,
        market_id: str,
        ts_ms: int
    ) -> "OrderBook":
        """
        Parse from CLOB REST API /book response.

        Expected format:
        {
            "bids": [{"price": "0.48", "size": "30"}, ...],
            "asks": [{"price": "0.52", "size": "25"}, ...]
        }
        """
        bids_raw = data.get("bids", [])
        asks_raw = data.get("asks", [])

        bids = tuple(OrderBookLevel.from_dict(b) for b in bids_raw)
        asks = tuple(OrderBookLevel.from_dict(a) for a in asks_raw)

        return cls(
            token_id=token_id,
            market_id=market_id,
            bids=bids,
            asks=asks,
            ts_ms=ts_ms,
        )
