"""
Common types and utilities shared across all models.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict
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
    
    @classmethod
    def from_string(cls, s: str) -> "Side":
        """Parse side from string (case-insensitive)."""
        s_lower = s.lower()
        if s_lower in ("buy", "bid"):
            return cls.BUY
        elif s_lower in ("sell", "ask"):
            return cls.SELL
        raise ValueError(f"Unknown side: {s}")


def current_ts_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


class SerializableMixin:
    """Mixin providing JSON serialization helpers."""
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        raise NotImplementedError("Subclasses must implement to_dict()")
