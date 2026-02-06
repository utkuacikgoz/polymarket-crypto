"""
Health monitoring models for connectors.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Any

from models.common import current_ts_ms


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
    
    @property
    def seconds_since_heartbeat(self) -> float:
        """Get seconds since last heartbeat."""
        return (current_ts_ms() - self.last_heartbeat_ts_ms) / 1000
    
    @property
    def seconds_since_message(self) -> Optional[float]:
        """Get seconds since last message."""
        if self.last_message_ts_ms is None:
            return None
        return (current_ts_ms() - self.last_message_ts_ms) / 1000
    
    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Check if connector is stale (no recent activity)."""
        return self.seconds_since_heartbeat > max_age_seconds


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
    
    @classmethod
    def connected(cls, connector_name: str, message: str = "Connected") -> "HealthEvent":
        """Create a connected health event."""
        return cls(
            ts_ms=current_ts_ms(),
            connector_name=connector_name,
            event_type="connected",
            message=message,
        )
    
    @classmethod
    def disconnected(cls, connector_name: str, message: str = "Disconnected") -> "HealthEvent":
        """Create a disconnected health event."""
        return cls(
            ts_ms=current_ts_ms(),
            connector_name=connector_name,
            event_type="disconnected",
            message=message,
        )
    
    @classmethod
    def error(cls, connector_name: str, message: str, details: Optional[Dict[str, Any]] = None) -> "HealthEvent":
        """Create an error health event."""
        return cls(
            ts_ms=current_ts_ms(),
            connector_name=connector_name,
            event_type="error",
            message=message,
            details=details,
        )
    
    @classmethod
    def heartbeat(cls, connector_name: str) -> "HealthEvent":
        """Create a heartbeat health event."""
        return cls(
            ts_ms=current_ts_ms(),
            connector_name=connector_name,
            event_type="heartbeat",
            message="Heartbeat",
        )
    
    @classmethod
    def reconnecting(cls, connector_name: str, attempt: int) -> "HealthEvent":
        """Create a reconnecting health event."""
        return cls(
            ts_ms=current_ts_ms(),
            connector_name=connector_name,
            event_type="reconnecting",
            message=f"Reconnecting (attempt {attempt})",
            details={"attempt": attempt},
        )
