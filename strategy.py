"""
Strategy framework for processing market data updates.

Provides a base strategy class that receives unified market data snapshots
from the ConnectorOrchestrator's data collector loop. Subclass BaseStrategy
and override `on_market_data_update` to implement option pricing or other logic.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from logging_utils import get_logger
from models import (
    ChainlinkPriceTick,
    MarketPriceTick,
    MarketSpec,
    PriceTick,
)

logger = get_logger("strategy")


@dataclass
class MarketDataUpdate:
    """
    A unified snapshot of all market data at a point in time.

    Aggregates data from every connector source so the strategy can
    inspect a consistent view without touching the orchestrator directly.

    Attributes:
        timestamp_ms: Timestamp when the snapshot was created (epoch ms).
        binance_ticks: Latest Binance ticks indexed by normalised coin (e.g. "BTC").
        chainlink_ticks: Latest Chainlink oracle ticks indexed by normalised coin.
        polymarket_ticks: Latest Polymarket price ticks indexed by token_id.
        markets: Active markets indexed by market_id.
        markets_by_series: Active markets indexed by series_key (e.g. "BTC-15M").
        polymarket_by_series: Polymarket ticks grouped by series_key → {UP/DOWN: tick}.
        usdt_usd_rate: Current USDT/USD conversion rate (None if unavailable).
        usdt_usd_tick: Raw USDCUSDT tick used for conversion.
        trigger_source: Which data source caused this update
                        ("binance", "polymarket", "chainlink", "market_spec", or "mixed").
    """

    timestamp_ms: int
    binance_ticks: dict[str, PriceTick] = field(default_factory=dict)
    chainlink_ticks: dict[str, ChainlinkPriceTick] = field(default_factory=dict)
    polymarket_ticks: dict[str, MarketPriceTick] = field(default_factory=dict)
    markets: dict[str, MarketSpec] = field(default_factory=dict)
    markets_by_series: dict[str, MarketSpec] = field(default_factory=dict)
    polymarket_by_series: dict[str, dict[str, MarketPriceTick]] = field(default_factory=dict)
    usdt_usd_rate: float | None = None
    usdt_usd_tick: PriceTick | None = None
    trigger_source: str = "mixed"

    # ── convenience helpers ──────────────────────────────────────────────

    def binance_price_usd(self, coin: str) -> float | None:
        """Return the Binance mid-price for *coin* converted to USD."""
        tick = self.binance_ticks.get(coin.upper())
        if tick and self.usdt_usd_rate:
            return tick.mid * self.usdt_usd_rate
        return None

    def polymarket_up_down(
        self, series_key: str
    ) -> tuple:
        """Return (up_tick, down_tick) for a series, either may be None."""
        bucket = self.polymarket_by_series.get(series_key.upper(), {})
        return bucket.get("UP"), bucket.get("DOWN")

    def series_keys(self) -> list[str]:
        """Return sorted list of available series keys."""
        return sorted(self.markets_by_series.keys())

    def unified_snapshot(self, series_key: str) -> dict[str, Any]:
        """Build a dict identical to ``ConnectorOrchestrator.get_unified_snapshot``."""
        series_key = series_key.upper()
        market = self.markets_by_series.get(series_key)
        parts = series_key.split("-")
        coin = parts[0] if parts else None
        duration = parts[1] if len(parts) > 1 else None
        pm = self.polymarket_by_series.get(series_key, {})

        return {
            "series_key": series_key,
            "coin": coin,
            "duration": duration,
            "market": market,
            "expiry_ts_ms": market.expiry_ts_ms if market else None,
            "binance": self.binance_ticks.get(coin) if coin else None,
            "binance_usd": self.binance_price_usd(coin) if coin else None,
            "usdt_usd": self.usdt_usd_rate,
            "chainlink": self.chainlink_ticks.get(coin) if coin else None,
            "polymarket_up": pm.get("UP"),
            "polymarket_down": pm.get("DOWN"),
            "timestamp_ms": self.timestamp_ms,
        }


class BaseStrategy(ABC):
    """
    Abstract base for all strategies.

    Lifecycle
    ---------
    1. ``on_start()``  – called once when the orchestrator starts.
    2. ``on_market_data_update(update)`` – called every time the data collector
       loop processes new events from any connector.
    3. ``on_stop()``   – called once during graceful shutdown.

    Subclass this and override at least ``on_market_data_update``.
    """

    def __init__(self, name: str = "strategy"):
        self.name = name
        self._logger = get_logger(name)

    # ── lifecycle hooks ──────────────────────────────────────────────────

    def on_start(self) -> None:
        """Called once when the orchestrator starts.

        Override for one-time initialisation (load models, open files, etc.).
        """
        self._logger.info(f"Strategy '{self.name}' started")

    @abstractmethod
    def on_market_data_update(self, update: MarketDataUpdate) -> None:
        """Called each time the data collector loop processes new events.

        Args:
            update: A frozen snapshot of the latest data across all sources.
        """
        ...

    def on_stop(self) -> None:
        """Called once during graceful shutdown.

        Override for cleanup (flush logs, close files, etc.).
        """
        self._logger.info(f"Strategy '{self.name}' stopped")


# =============================================================================
# Example / placeholder strategy – replace with option-pricing logic later
# =============================================================================


class LoggingStrategy(BaseStrategy):
    """
    A minimal strategy that logs every data update.

    Useful for debugging and as a template for real strategies.
    """

    def __init__(self):
        super().__init__(name="logging-strategy")
        self._update_count = 0

    def on_market_data_update(self, update: MarketDataUpdate) -> None:
        self._update_count += 1

        if self._update_count % 100 == 0:
            now = datetime.now(UTC).strftime("%H:%M:%S")
            series = ", ".join(update.series_keys()) or "none"
            self._logger.info(
                f"[{now}] update #{self._update_count} | "
                f"trigger={update.trigger_source} | "
                f"binance={len(update.binance_ticks)} | "
                f"chainlink={len(update.chainlink_ticks)} | "
                f"polymarket={len(update.polymarket_ticks)} | "
                f"markets={len(update.markets)} | "
                f"series=[{series}]"
            )
