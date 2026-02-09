"""
RTDS (Real-Time Data Service) models for Chainlink price feeds.

Models for Polymarket RTDS WebSocket data, specifically Chainlink oracle prices.
Docs: https://docs.polymarket.com/developers/RTDS/RTDS-crypto-prices
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from enum import Enum


class RTDSSource(Enum):
    """Source type for RTDS price data."""
    CHAINLINK = "chainlink"
    BINANCE = "binance"


@dataclass(frozen=True)
class ChainlinkPriceTick:
    """
    A single price tick from Chainlink oracle via RTDS.
    
    Chainlink prices are oracle-based reference prices, typically
    updated less frequently than exchange prices but considered
    more reliable for settlement purposes.
    
    Attributes:
        ts_ms: Timestamp when the message was received (epoch milliseconds)
        price_ts_ms: Timestamp when the price was recorded by Chainlink
        symbol: Trading pair symbol in Chainlink format (e.g., "eth/usd", "btc/usd")
        price: Current price value in the quote currency (USD)
        source: Source identifier
    """
    ts_ms: int
    price_ts_ms: int
    symbol: str
    price: float
    source: RTDSSource = RTDSSource.CHAINLINK

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "type": "chainlink_price_tick",
            "ts_ms": self.ts_ms,
            "price_ts_ms": self.price_ts_ms,
            "symbol": self.symbol,
            "price": self.price,
            "source": self.source.value
        }

    @classmethod
    def from_rtds_message(cls, data: Dict[str, Any], received_ts_ms: int) -> "ChainlinkPriceTick":
        """
        Create ChainlinkPriceTick from RTDS WebSocket message.
        
        RTDS message format:
        {
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": 1753314064237,
            "payload": {
                "symbol": "eth/usd",
                "timestamp": 1753314064213,
                "value": 3456.78
            }
        }
        
        Args:
            data: The full RTDS message dict
            received_ts_ms: Timestamp when message was received
            
        Returns:
            ChainlinkPriceTick instance
        """
        payload = data.get("payload", {})
        return cls(
            ts_ms=received_ts_ms,
            price_ts_ms=payload.get("timestamp", received_ts_ms),
            symbol=payload.get("symbol", "unknown"),
            price=float(payload.get("value", 0.0)),
            source=RTDSSource.CHAINLINK
        )

    @property
    def base_currency(self) -> str:
        """Extract base currency from symbol (e.g., 'eth' from 'eth/usd')."""
        if "/" in self.symbol:
            return self.symbol.split("/")[0].upper()
        return self.symbol.upper()
    
    @property
    def quote_currency(self) -> str:
        """Extract quote currency from symbol (e.g., 'usd' from 'eth/usd')."""
        if "/" in self.symbol:
            return self.symbol.split("/")[1].upper()
        return "USD"


@dataclass(frozen=True)
class RTDSSubscription:
    """
    Represents an RTDS subscription request.
    
    Used to build subscription messages for the WebSocket.
    """
    topic: str
    type: str = "*"
    filters: str = ""
    
    def to_subscribe_message(self) -> Dict[str, Any]:
        """Create the subscribe message for RTDS WebSocket.
        
        Format matches official TypeScript client:
        {
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": ""  // or '{"symbol":"eth/usd"}'
            }]
        }
        """
        return {
            "action": "subscribe",
            "subscriptions": [{
                "topic": self.topic,
                "type": self.type,
                "filters": self.filters  # Always include, empty string for all
            }]
        }
    
    @classmethod
    def chainlink_all(cls) -> "RTDSSubscription":
        """Create subscription for all Chainlink prices."""
        return cls(
            topic="crypto_prices_chainlink",
            type="*",
            filters=""
        )
    
    @classmethod
    def chainlink_symbol(cls, symbol: str) -> "RTDSSubscription":
        """
        Create subscription for a specific Chainlink symbol.
        
        Args:
            symbol: Symbol in Chainlink format (e.g., "eth/usd")
            
        Returns:
            RTDSSubscription for the specific symbol
        """
        import json
        filters = json.dumps({"symbol": symbol.lower()})
        return cls(
            topic="crypto_prices_chainlink",
            type="*",
            filters=filters
        )


def parse_rtds_message(data: Dict[str, Any], received_ts_ms: int) -> Optional[ChainlinkPriceTick]:
    """
    Parse an RTDS WebSocket message into a typed model.
    
    Args:
        data: Parsed JSON message from WebSocket
        received_ts_ms: Timestamp when message was received
        
    Returns:
        ChainlinkPriceTick if message is a Chainlink price update, None otherwise
    """
    topic = data.get("topic", "")
    msg_type = data.get("type", "")
    
    if topic == "crypto_prices_chainlink" and msg_type == "update":
        return ChainlinkPriceTick.from_rtds_message(data, received_ts_ms)
    
    return None
