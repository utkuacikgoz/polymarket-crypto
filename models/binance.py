"""
Binance-specific data models.

Models for Binance WebSocket and REST API data.
"""

from dataclasses import dataclass
from typing import Dict, Any
import time

from models.common import SourceType


@dataclass(frozen=True)
class PriceTick:
    """
    A single price tick from Binance spot exchange.
    
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
    source: SourceType = SourceType.BINANCE

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
        """
        Create PriceTick from Binance bookTicker message.
        
        Binance bookTicker format:
        {
            "u": 400900217,     // order book updateId
            "s": "BTCUSDT",     // symbol
            "b": "25.35190000", // best bid price
            "B": "31.21000000", // best bid qty
            "a": "25.36520000", // best ask price
            "A": "40.66000000"  // best ask qty
        }
        """
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

    @property
    def spread(self) -> float:
        """Calculate bid-ask spread."""
        return self.ask - self.bid
    
    @property
    def spread_bps(self) -> float:
        """Calculate bid-ask spread in basis points."""
        if self.mid == 0:
            return 0.0
        return (self.spread / self.mid) * 10000
