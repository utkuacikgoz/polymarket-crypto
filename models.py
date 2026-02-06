"""
Data models for the market data connectors.

All models are immutable dataclasses with serialization helpers for JSONL logging.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any
from enum import Enum
import time


class SourceType(Enum):
    """Source identifier for price data."""
    BINANCE = "binance"
    POLYMARKET_WS = "polymarket_ws"
    POLYMARKET_REST = "polymarket_rest"
    GAMMA = "gamma"


class MarketStatus(Enum):
    """Status of a Polymarket market."""
    ACTIVE = "active"
    CLOSED = "closed"
    RESOLVED = "resolved"
    UNKNOWN = "unknown"


class Side(Enum):
    """Order side."""
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class PriceTick:
    """
    A single price tick from a spot exchange (e.g., Binance).
    
    Attributes:
        ts_ms: Timestamp in milliseconds (epoch)
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        bid: Best bid price
        ask: Best ask price
        mid: Mid-price computed as (bid + ask) / 2
        source: Source of the tick data
    """
    ts_ms: int
    symbol: str
    bid: float
    ask: float
    mid: float
    source: SourceType

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "price_tick",
            "ts_ms": self.ts_ms,
            "symbol": self.symbol,
            "bid": self.bid,
            "ask": self.ask,
            "mid": self.mid,
            "source": self.source.value
        }

    @classmethod
    def from_binance_book_ticker(cls, data: Dict[str, Any]) -> "PriceTick":
        """Create PriceTick from Binance bookTicker message."""
        bid = float(data["b"])
        ask = float(data["a"])
        return cls(
            ts_ms=int(time.time() * 1000),
            symbol=data["s"],
            bid=bid,
            ask=ask,
            mid=(bid + ask) / 2,
            source=SourceType.BINANCE
        )


@dataclass(frozen=True)
class MarketSpec:
    """
    Specification of a Polymarket market.
    
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
    strike: Optional[float]
    token_yes: str
    token_no: str
    status: MarketStatus
    is_active: bool = True
    accepts_orders: bool = True
    outcomes: tuple = field(default=("Yes", "No"))
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
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
    def from_gamma_response(cls, data: Dict[str, Any]) -> Optional["MarketSpec"]:
        """
        Parse MarketSpec from Gamma API market response.
        
        Returns None if required fields are missing.
        """
        try:
            import json
            
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
                import json
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
        except (KeyError, ValueError, IndexError) as e:
            return None


@dataclass(frozen=True)
class MarketPriceTick:
    """
    A single price tick from Polymarket for a specific token.
    
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
    side: Optional[Side]
    source: SourceType

    def to_dict(self) -> Dict[str, Any]:
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


@dataclass(frozen=True)
class MarketSnapshot:
    """
    A complete snapshot of a Polymarket market's current state.
    
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
    best_yes_price: Optional[float]
    best_no_price: Optional[float]
    best_yes_bid: Optional[float] = None
    best_yes_ask: Optional[float] = None
    best_no_bid: Optional[float] = None
    best_no_ask: Optional[float] = None
    source: SourceType = SourceType.POLYMARKET_WS

    def to_dict(self) -> Dict[str, Any]:
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


@dataclass(frozen=True)
class ConnectorHealth:
    """
    Health status of a connector.
    
    Attributes:
        name: Connector name/identifier
        healthy: Whether the connector is healthy
        last_heartbeat_ts_ms: Timestamp of last successful heartbeat
        last_error: Last error message (if any)
        reconnect_count: Number of reconnection attempts
        connected: Whether currently connected
        last_message_ts_ms: Timestamp of last message received
    """
    name: str
    healthy: bool
    last_heartbeat_ts_ms: int
    last_error: Optional[str]
    reconnect_count: int
    connected: bool = False
    last_message_ts_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "connector_health",
            "name": self.name,
            "healthy": self.healthy,
            "last_heartbeat_ts_ms": self.last_heartbeat_ts_ms,
            "last_error": self.last_error,
            "reconnect_count": self.reconnect_count,
            "connected": self.connected,
            "last_message_ts_ms": self.last_message_ts_ms
        }


@dataclass(frozen=True)
class HealthEvent:
    """
    Health event emitted by connectors for monitoring.
    
    Attributes:
        ts_ms: Event timestamp
        connector_name: Name of the connector
        event_type: Type of health event
        message: Human-readable message
        details: Additional details
    """
    ts_ms: int
    connector_name: str
    event_type: str  # "connected", "disconnected", "error", "heartbeat", "reconnecting"
    message: str
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "health_event",
            "ts_ms": self.ts_ms,
            "connector_name": self.connector_name,
            "event_type": self.event_type,
            "message": self.message,
            "details": self.details
        }


def current_ts_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)
