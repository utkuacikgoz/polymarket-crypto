#!/usr/bin/env python3
"""
Main application orchestrator for market data connectors.

Wires together all connectors, manages health monitoring,
and provides a unified interface for running the system.
"""

import signal
import sys
import time
from datetime import UTC, datetime
from threading import Event, Lock, Thread
from typing import Any

from config import AppConfig, get_config
from connectors.binance_ws import BinanceWebSocketConnector
from connectors.polymarket_clob_rest import PolymarketClobRestConnector
from connectors.polymarket_clob_ws import PolymarketClobWebSocketConnector
from connectors.polymarket_gamma import PolymarketGammaConnector
from connectors.rtds_chainlink import RTDSChainlinkConnector
from logging_utils import get_logger, setup_logging
from models import (
    ChainlinkPriceTick,
    ConnectorHealth,
    MarketPriceTick,
    MarketSpec,
    PriceTick,
    current_ts_ms,
)
from pubsub import (
    TOPIC_BINANCE_TICKS,
    TOPIC_CHAINLINK_PRICES,
    TOPIC_HEALTH,
    TOPIC_MARKET_SPEC,
    TOPIC_POLYMARKET_PRICES,
    Subscription,
    subscribe,
)
from strategy import BaseStrategy, MarketDataUpdate

logger = get_logger("app")


class ConnectorOrchestrator:
    """
    Orchestrates all connectors and manages system lifecycle.

    Responsibilities:
    - Initialize and start all connectors
    - Monitor connector health
    - Handle fallback from WebSocket to REST
    - Print periodic status updates
    - Graceful shutdown on signals
    """

    def __init__(
        self,
        config: AppConfig | None = None,
        strategies: list["BaseStrategy"] | None = None,
    ):
        """
        Initialize the orchestrator.

        Args:
            config: Application configuration. If None, loads from environment.
            strategies: Optional list of strategy instances to receive data updates.
        """
        self.config = config or get_config()
        self._strategies: list[BaseStrategy] = list(strategies or [])

        # Connectors
        self._binance_ws: BinanceWebSocketConnector | None = None
        self._gamma: PolymarketGammaConnector | None = None
        self._clob_ws: PolymarketClobWebSocketConnector | None = None
        self._clob_rest: PolymarketClobRestConnector | None = None
        self._rtds_chainlink: RTDSChainlinkConnector | None = None

        # State
        self._running = False
        self._stop_event = Event()
        self._lock = Lock()

        # Health tracking
        self._rest_fallback_active = False
        self._ws_unhealthy_since: float | None = None

        # Subscriptions for monitoring
        self._health_subscription: Subscription | None = None
        self._binance_subscription: Subscription | None = None
        self._polymarket_subscription: Subscription | None = None
        self._market_subscription: Subscription | None = None
        self._chainlink_subscription: Subscription | None = None

        # Latest data for status display (multi-symbol/multi-market)
        self._last_binance_ticks: dict[str, PriceTick] = {}  # By normalized symbol (e.g., "BTC")
        self._last_polymarket_ticks: dict[str, MarketPriceTick] = {}  # By token_id
        self._last_chainlink_ticks: dict[str, ChainlinkPriceTick] = {}  # By normalized symbol (e.g., "BTC")
        self._current_markets: dict[str, MarketSpec] = {}  # By market_id

        # USDT/USD conversion rate from Binance (using USDCUSDT as proxy)
        # usdt_usd = 1 / usdcusdt_mid_price (since USDC ≈ USD)
        self._usdt_usd_tick: PriceTick | None = None

        # Normalized indexes for easy data access
        self._markets_by_series_key: dict[str, MarketSpec] = {}  # By series_key (e.g., "BTC-15M")
        self._markets_by_expiry: dict[str, MarketSpec] = {}  # By expiry_key (e.g., "BTC-15M-1707408000000")
        self._polymarket_ticks_by_series: dict[str, dict[str, MarketPriceTick]] = {}  # series_key -> {UP/DOWN: tick}

        # Background threads
        self._status_thread: Thread | None = None
        self._health_monitor_thread: Thread | None = None
        self._data_collector_thread: Thread | None = None

    def start(self) -> None:
        """
        Start all connectors and monitoring.
        """
        with self._lock:
            if self._running:
                raise RuntimeError("Orchestrator already running")

            self._running = True
            self._stop_event.clear()

        logger.info("Starting connector orchestrator")

        # Initialize logging
        setup_logging(self.config.logging)

        # Create connectors
        self._binance_ws = BinanceWebSocketConnector(self.config.binance)
        self._gamma = PolymarketGammaConnector(self.config.gamma)
        self._clob_ws = PolymarketClobWebSocketConnector(self.config.clob)
        self._clob_rest = PolymarketClobRestConnector(self.config.clob)
        self._rtds_chainlink = RTDSChainlinkConnector(self.config.rtds, self.config.gamma)

        # Subscribe to event topics for monitoring
        self._health_subscription = subscribe(TOPIC_HEALTH, "orchestrator_health")
        self._binance_subscription = subscribe(TOPIC_BINANCE_TICKS, "orchestrator_binance")
        self._polymarket_subscription = subscribe(TOPIC_POLYMARKET_PRICES, "orchestrator_pm_prices")
        self._market_subscription = subscribe(TOPIC_MARKET_SPEC, "orchestrator_market")
        self._chainlink_subscription = subscribe(TOPIC_CHAINLINK_PRICES, "orchestrator_chainlink")

        # Start connectors
        logger.info("Starting Binance WebSocket connector")
        self._binance_ws.start()

        logger.info("Starting Polymarket Gamma connector")
        self._gamma.start()

        logger.info("Starting Polymarket CLOB WebSocket connector")
        self._clob_ws.start()

        logger.info("Starting RTDS Chainlink connector")
        self._rtds_chainlink.start()

        # REST connector is started on-demand when WS is unhealthy
        # self._clob_rest.start()

        # Notify strategies
        for strat in self._strategies:
            try:
                strat.on_start()
            except Exception as e:
                logger.error(f"Error starting strategy {strat.name}: {e}")

        # Start background threads
        self._start_background_threads()

        logger.info("All connectors started")

    def stop(self, timeout: float = 10.0) -> None:
        """
        Stop all connectors gracefully.

        Args:
            timeout: Maximum time to wait for shutdown
        """
        logger.info("Stopping connector orchestrator")

        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        # Notify strategies
        for strat in self._strategies:
            try:
                strat.on_stop()
            except Exception as e:
                logger.error(f"Error stopping strategy {strat.name}: {e}")

        # Stop background threads
        self._stop_background_threads(timeout / 2)

        # Stop connectors
        connectors = [
            self._clob_rest,
            self._clob_ws,
            self._rtds_chainlink,
            self._gamma,
            self._binance_ws
        ]

        for connector in connectors:
            if connector:
                try:
                    connector.stop(timeout=timeout / len(connectors))
                except Exception as e:
                    logger.error(f"Error stopping {connector.name}: {e}")

        # Cleanup subscriptions
        from pubsub import unsubscribe
        for sub in [
            self._health_subscription,
            self._binance_subscription,
            self._polymarket_subscription,
            self._market_subscription,
            self._chainlink_subscription
        ]:
            if sub:
                unsubscribe(sub)

        logger.info("Orchestrator stopped")

    def _start_background_threads(self) -> None:
        """Start background monitoring threads."""
        self._status_thread = Thread(
            target=self._status_loop,
            name="status-printer",
            daemon=True
        )
        self._status_thread.start()

        self._health_monitor_thread = Thread(
            target=self._health_monitor_loop,
            name="health-monitor",
            daemon=True
        )
        self._health_monitor_thread.start()

        self._data_collector_thread = Thread(
            target=self._data_collector_loop,
            name="data-collector",
            daemon=True
        )
        self._data_collector_thread.start()

    def _stop_background_threads(self, timeout: float) -> None:
        """Stop background threads."""
        threads = [
            self._status_thread,
            self._health_monitor_thread,
            self._data_collector_thread
        ]

        for thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=timeout / len(threads))

    def _status_loop(self) -> None:
        """Print periodic status updates."""
        while not self._stop_event.is_set():
            try:
                self._print_status()
            except Exception as e:
                logger.error(f"Error printing status: {e}")

            self._stop_event.wait(timeout=self.config.status_interval_sec)

    def _health_monitor_loop(self) -> None:
        """Monitor connector health and manage fallback."""
        while not self._stop_event.is_set():
            try:
                self._check_health()
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")

            self._stop_event.wait(timeout=self.config.health_check_interval_sec)

    def _data_collector_loop(self) -> None:
        """Collect latest data from subscriptions for status display."""
        from config import BinanceConfig

        while not self._stop_event.is_set():
            try:
                has_new_data = False
                trigger_sources: set = set()

                # Collect binance ticks (multi-symbol) - normalize to coin symbol (e.g., "BTC")
                if self._binance_subscription:
                    ticks = self._binance_subscription.get_all()
                    for tick in ticks:
                        if isinstance(tick, PriceTick):
                            symbol_upper = tick.symbol.upper()

                            # Handle USDT/USD proxy separately (USDCUSDT)
                            if symbol_upper == BinanceConfig.USDT_USD_PROXY.upper():
                                self._usdt_usd_tick = tick
                                #logger.info(f"[Binance USDT/USD] {tick}")
                            else:
                                # Normalize: BTCUSDT -> BTC
                                normalized = symbol_upper.replace("USDT", "")
                                self._last_binance_ticks[normalized] = tick
                                #logger.info(f"[Binance] {tick}")
                            has_new_data = True
                            trigger_sources.add("binance")

                # Collect polymarket ticks - also index by series key
                if self._polymarket_subscription:
                    ticks = self._polymarket_subscription.get_all()
                    for tick in ticks:
                        if isinstance(tick, MarketPriceTick):
                            self._last_polymarket_ticks[tick.token_id] = tick
                            # Index by series key if we can find the market
                            self._update_polymarket_series_index(tick)
                            has_new_data = True
                            trigger_sources.add("polymarket")

                # Collect market updates (multi-market) - index by series_key and expiry_key
                if self._market_subscription:
                    markets = self._market_subscription.get_all()
                    for market in markets:
                        if isinstance(market, MarketSpec):
                            self._current_markets[market.market_id] = market
                            has_new_data = True
                            trigger_sources.add("market_spec")
                            # Index by series_key (e.g., "BTC-15M")
                            if market.extra:
                                series_key = market.extra.get("series_key")
                                if series_key:
                                    self._markets_by_series_key[series_key] = market
                                    # Index by expiry_key (e.g., "BTC-15M-1707408000000")
                                    expiry_key = f"{series_key}-{market.expiry_ts_ms}"
                                    self._markets_by_expiry[expiry_key] = market
                            # Update REST connector with new market if in fallback mode
                            if self._clob_rest and self._rest_fallback_active:
                                self._clob_rest.add_market(market)

                # Collect Chainlink price ticks - normalize to coin symbol (e.g., "BTC")
                if self._chainlink_subscription:
                    ticks = self._chainlink_subscription.get_all()
                    for tick in ticks:
                        if isinstance(tick, ChainlinkPriceTick):
                            # Normalize: "btc/usd" -> "BTC"
                            normalized = tick.symbol.split("/")[0].upper()
                            self._last_chainlink_ticks[normalized] = tick
                            logger.info(f"[Chainlink] {tick}")
                            has_new_data = True
                            trigger_sources.add("chainlink")

                # ── Dispatch to strategies ───────────────────────────
                if has_new_data and self._strategies:
                    update = MarketDataUpdate(
                        timestamp_ms=current_ts_ms(),
                        binance_ticks=self._last_binance_ticks.copy(),
                        chainlink_ticks=self._last_chainlink_ticks.copy(),
                        polymarket_ticks=self._last_polymarket_ticks.copy(),
                        markets=self._current_markets.copy(),
                        markets_by_series=self._markets_by_series_key.copy(),
                        polymarket_by_series={
                            k: dict(v)
                            for k, v in self._polymarket_ticks_by_series.items()
                        },
                        usdt_usd_rate=self.get_usdt_usd_rate(),
                        usdt_usd_tick=self._usdt_usd_tick,
                        trigger_source=(
                            next(iter(trigger_sources))
                            if len(trigger_sources) == 1
                            else "mixed"
                        ),
                    )
                    for strat in self._strategies:
                        try:
                            strat.on_market_data_update(update)
                        except Exception as e:
                            logger.error(
                                f"Strategy {strat.name} error: {e}", exc_info=True
                            )

            except Exception as e:
                logger.debug(f"Error collecting data: {e}")

            #time.sleep(0.001)

    def _update_polymarket_series_index(self, tick: MarketPriceTick) -> None:
        """Update the Polymarket series index for easy access by series_key."""
        for market in self._current_markets.values():
            if tick.token_id == market.token_yes:
                series_key = market.extra.get("series_key") if market.extra else None
                if series_key:
                    if series_key not in self._polymarket_ticks_by_series:
                        self._polymarket_ticks_by_series[series_key] = {}
                    self._polymarket_ticks_by_series[series_key]["UP"] = tick
                return
            elif tick.token_id == market.token_no:
                series_key = market.extra.get("series_key") if market.extra else None
                if series_key:
                    if series_key not in self._polymarket_ticks_by_series:
                        self._polymarket_ticks_by_series[series_key] = {}
                    self._polymarket_ticks_by_series[series_key]["DOWN"] = tick
                return

    def _check_health(self) -> None:
        """
        Check connector health and activate fallback if needed.
        """
        if not self._clob_ws:
            return

        ws_healthy = self._clob_ws.is_healthy()
        current_time = time.time()

        if not ws_healthy:
            if self._ws_unhealthy_since is None:
                self._ws_unhealthy_since = current_time
                logger.warning("Polymarket WebSocket became unhealthy")

            unhealthy_duration = current_time - self._ws_unhealthy_since

            # Activate REST fallback if WS unhealthy for threshold duration
            if unhealthy_duration >= self.config.clob.ws_unhealthy_threshold_sec:
                if not self._rest_fallback_active:
                    self._activate_rest_fallback()
        else:
            if self._ws_unhealthy_since is not None:
                logger.info("Polymarket WebSocket recovered")
                self._ws_unhealthy_since = None

            # Deactivate REST fallback when WS is healthy
            if self._rest_fallback_active:
                self._deactivate_rest_fallback()

    def _activate_rest_fallback(self) -> None:
        """Activate REST polling fallback."""
        logger.warning("Activating REST fallback for Polymarket prices")

        self._rest_fallback_active = True

        # Configure REST connector with current markets
        if self._current_markets and self._clob_rest:
            for market in self._current_markets.values():
                self._clob_rest.add_market(market)
            self._clob_rest.start()

    def _deactivate_rest_fallback(self) -> None:
        """Deactivate REST polling fallback."""
        logger.info("Deactivating REST fallback - WebSocket is healthy")

        self._rest_fallback_active = False

        if self._clob_rest:
            self._clob_rest.stop()

    def _print_status(self) -> None:
        """Print a status line to console showing unified per-asset pricing."""
        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        # USDT/USD rate for conversion
        usdt_usd = self.get_usdt_usd_rate()
        usdt_str = f"USDT/USD:{usdt_usd:.6f}" if usdt_usd else "USDT/USD:N/A"

        # Build unified per-asset status
        # Group by series_key (e.g., "BTC-15M", "ETH-15M")
        asset_lines = []

        for series_key in sorted(set(
            m.extra.get("series_key", "Unknown")
            for m in self._current_markets.values()
            if m.extra
        )):
            # Find markets for this series
            series_markets = [
                m for m in self._current_markets.values()
                if m.extra and m.extra.get("series_key") == series_key
            ]

            if not series_markets:
                continue

            market = series_markets[0]  # Take first market for this series
            coin = series_key.split("-")[0] if "-" in series_key else series_key

            # Get Binance price for this coin (now indexed by normalized symbol)
            # Show both USDT and USD-converted prices
            binance_tick = self._last_binance_ticks.get(coin.upper())
            if binance_tick:
                bin_age = current_ts_ms() - binance_tick.ts_ms
                usd_price = binance_tick.mid * usdt_usd if usdt_usd else None
                if usd_price:
                    bin_str = f"Bin:${usd_price:,.2f}(USDT:{binance_tick.mid:,.2f})({bin_age}ms)"
                else:
                    bin_str = f"Bin:USDT:{binance_tick.mid:,.2f}({bin_age}ms)"
            else:
                bin_str = "Bin:N/A"

            # Get Chainlink price for this coin (now indexed by normalized symbol)
            chainlink_tick = self._last_chainlink_ticks.get(coin.upper())
            if chainlink_tick:
                cl_age = current_ts_ms() - chainlink_tick.ts_ms
                cl_str = f"CL:${chainlink_tick.price:,.2f}({cl_age}ms)"
            else:
                cl_str = "CL:N/A"

            # Get Polymarket prices for UP/DOWN tokens
            pm_parts = []
            for token_id in market.get_token_ids():
                pm_tick = self._last_polymarket_ticks.get(token_id)
                if pm_tick:
                    side = "UP" if token_id == market.token_yes else "DOWN"
                    pm_age = current_ts_ms() - pm_tick.ts_ms
                    # Format: side:price (age)
                    pm_parts.append(f"{side}:{pm_tick.price:.3f}({pm_age}ms)")
            pm_str = " ".join(pm_parts) if pm_parts else "PM:N/A"

            # Get expiry time
            expiry_dt = datetime.fromtimestamp(market.expiry_ts_ms / 1000, tz=UTC)
            expiry = expiry_dt.strftime("%H:%M:%S")

            asset_lines.append(f"[{series_key} exp:{expiry}] {bin_str} | {cl_str} | {pm_str}")

        # Health summary
        health = self._get_health_summary()

        # Fallback status
        fallback = " [REST FALLBACK]" if self._rest_fallback_active else ""

        # Print header line with USDT/USD rate
        header = f"[{now}] {usdt_str} | Health: {health}{fallback}"
        print(header)

        # Print each asset on its own line for readability
        if asset_lines:
            for line in asset_lines:
                print(f"  {line}")
        else:
            print("  No active markets")

    def _get_health_summary(self) -> str:
        """Get a short health summary string."""
        statuses = []

        connectors = [
            ("BIN", self._binance_ws),
            ("GAM", self._gamma),
            ("WS", self._clob_ws),
            ("RTDS", self._rtds_chainlink),
            ("REST", self._clob_rest if self._rest_fallback_active else None),
        ]

        for name, connector in connectors:
            if connector:
                status = "✓" if connector.is_healthy() else "✗"
                statuses.append(f"{name}:{status}")

        return " ".join(statuses)

    def get_all_health(self) -> dict[str, ConnectorHealth]:
        """Get health status for all connectors."""
        health = {}

        connectors = [
            self._binance_ws,
            self._gamma,
            self._clob_ws,
            self._rtds_chainlink,
            self._clob_rest
        ]

        for connector in connectors:
            if connector:
                health[connector.name] = connector.get_health()

        return health

    # =========================================================================
    # Normalized Data Accessors
    # =========================================================================

    def get_usdt_usd_rate(self) -> float | None:
        """Get the current USDT/USD exchange rate.

        Calculated from USDCUSDT: usdt_usd = 1 / usdcusdt_mid
        Since USDC tracks USD closely, this gives USDT price in USD terms.

        Returns:
            USDT/USD rate or None if not available
        """
        if self._usdt_usd_tick and self._usdt_usd_tick.mid > 0:
            return 1.0 / self._usdt_usd_tick.mid
        return None

    @property
    def usdt_usd_tick(self) -> PriceTick | None:
        """Get the raw USDCUSDT tick for USDT/USD conversion."""
        return self._usdt_usd_tick

    def get_binance_price(self, coin: str) -> PriceTick | None:
        """Get latest Binance price tick for a coin.

        Args:
            coin: Normalized coin symbol (e.g., "BTC", "ETH")

        Returns:
            Latest PriceTick or None if not available
        """
        return self._last_binance_ticks.get(coin.upper())

    def get_chainlink_price(self, coin: str) -> ChainlinkPriceTick | None:
        """Get latest Chainlink oracle price for a coin.

        Args:
            coin: Normalized coin symbol (e.g., "BTC", "ETH")

        Returns:
            Latest ChainlinkPriceTick or None if not available
        """
        return self._last_chainlink_ticks.get(coin.upper())

    def get_binance_price_usd(self, coin: str) -> float | None:
        """Get latest Binance price converted to USD.

        Calculates: coin_usd = coin_usdt * usdt_usd

        Args:
            coin: Normalized coin symbol (e.g., "BTC", "ETH")

        Returns:
            Mid price in USD or None if data not available
        """
        tick = self._last_binance_ticks.get(coin.upper())
        usdt_usd = self.get_usdt_usd_rate()

        if tick and usdt_usd:
            return tick.mid * usdt_usd
        return None

    def get_market_by_series(self, series_key: str) -> MarketSpec | None:
        """Get the current market for a series.

        Args:
            series_key: Series identifier (e.g., "BTC-15M", "ETH-1H")

        Returns:
            Latest MarketSpec for the series or None if not available
        """
        return self._markets_by_series_key.get(series_key.upper())

    def get_market_by_expiry(self, expiry_key: str) -> MarketSpec | None:
        """Get a specific market by expiry key.

        Args:
            expiry_key: Expiry identifier (e.g., "BTC-15M-1707408000000")

        Returns:
            MarketSpec for the specific expiry or None if not available
        """
        return self._markets_by_expiry.get(expiry_key.upper())

    def get_polymarket_prices(self, series_key: str) -> dict[str, MarketPriceTick] | None:
        """Get Polymarket UP/DOWN prices for a series.

        Args:
            series_key: Series identifier (e.g., "BTC-15M", "ETH-1H")

        Returns:
            Dict with "UP" and "DOWN" keys mapping to MarketPriceTick, or None
        """
        return self._polymarket_ticks_by_series.get(series_key.upper())

    def get_all_prices_for_coin(self, coin: str) -> dict[str, Any]:
        """Get all available prices for a coin from all sources.

        Args:
            coin: Normalized coin symbol (e.g., "BTC", "ETH")

        Returns:
            Dict containing:
                - "binance": PriceTick or None
                - "chainlink": ChainlinkPriceTick or None
                - "markets": Dict[series_key, MarketSpec]
                - "polymarket": Dict[series_key, Dict[UP/DOWN, MarketPriceTick]]
        """
        coin = coin.upper()

        # Find all series for this coin
        coin_series_keys = [
            key for key in self._markets_by_series_key.keys()
            if key.startswith(f"{coin}-")
        ]

        markets = {key: self._markets_by_series_key[key] for key in coin_series_keys}
        pm_prices = {
            key: self._polymarket_ticks_by_series.get(key)
            for key in coin_series_keys
            if key in self._polymarket_ticks_by_series
        }

        return {
            "binance": self._last_binance_ticks.get(coin),
            "chainlink": self._last_chainlink_ticks.get(coin),
            "markets": markets,
            "polymarket": pm_prices,
        }

    def get_unified_snapshot(self, series_key: str) -> dict[str, Any] | None:
        """Get a unified snapshot of all price data for a series.

        This is the primary method for accessing all relevant data for a
        trading decision on a specific series.

        Args:
            series_key: Series identifier (e.g., "BTC-15M", "ETH-1H")

        Returns:
            Dict containing:
                - "series_key": str
                - "coin": str (e.g., "BTC")
                - "duration": str (e.g., "15M")
                - "market": MarketSpec or None
                - "expiry_ts_ms": int or None
                - "binance": PriceTick or None (USDT price)
                - "binance_usd": float or None (USD-converted price)
                - "usdt_usd": float or None (USDT/USD rate)
                - "chainlink": ChainlinkPriceTick or None (USD price)
                - "polymarket_up": MarketPriceTick or None
                - "polymarket_down": MarketPriceTick or None
                - "timestamp_ms": int (current timestamp)
        """
        series_key = series_key.upper()
        market = self._markets_by_series_key.get(series_key)

        # Parse coin from series_key
        parts = series_key.split("-")
        coin = parts[0] if parts else None
        duration = parts[1] if len(parts) > 1 else None

        pm_prices = self._polymarket_ticks_by_series.get(series_key, {})

        # Get USD-converted Binance price
        binance_usd = self.get_binance_price_usd(coin) if coin else None

        return {
            "series_key": series_key,
            "coin": coin,
            "duration": duration,
            "market": market,
            "expiry_ts_ms": market.expiry_ts_ms if market else None,
            "binance": self._last_binance_ticks.get(coin) if coin else None,
            "binance_usd": binance_usd,
            "usdt_usd": self.get_usdt_usd_rate(),
            "chainlink": self._last_chainlink_ticks.get(coin) if coin else None,
            "polymarket_up": pm_prices.get("UP"),
            "polymarket_down": pm_prices.get("DOWN"),
            "timestamp_ms": current_ts_ms(),
        }

    @property
    def subscribed_coins(self) -> tuple:
        """Get the list of coins we're subscribed to on Polymarket."""
        return self.config.get_subscribed_coins()

    @property
    def all_binance_prices(self) -> dict[str, PriceTick]:
        """Get all latest Binance prices indexed by normalized coin symbol."""
        return self._last_binance_ticks.copy()

    @property
    def all_chainlink_prices(self) -> dict[str, ChainlinkPriceTick]:
        """Get all latest Chainlink prices indexed by normalized coin symbol."""
        return self._last_chainlink_ticks.copy()

    @property
    def all_markets(self) -> dict[str, MarketSpec]:
        """Get all current markets indexed by market_id."""
        return self._current_markets.copy()

    @property
    def all_markets_by_series(self) -> dict[str, MarketSpec]:
        """Get all current markets indexed by series_key (e.g., 'BTC-15M')."""
        return self._markets_by_series_key.copy()

    # =========================================================================
    # Strategy management
    # =========================================================================

    def add_strategy(self, strategy: "BaseStrategy") -> None:
        """Register a strategy to receive market data updates.

        Can be called before or after start(). If the orchestrator is
        already running the strategy's on_start() will be invoked
        immediately.

        Args:
            strategy: Strategy instance to add.
        """
        self._strategies.append(strategy)
        logger.info(f"Added strategy: {strategy.name}")
        if self._running:
            try:
                strategy.on_start()
            except Exception as e:
                logger.error(f"Error starting strategy {strategy.name}: {e}")

    def remove_strategy(self, strategy: "BaseStrategy") -> None:
        """Un-register a strategy.

        Calls on_stop() and removes it from the update list.
        """
        if strategy in self._strategies:
            try:
                strategy.on_stop()
            except Exception as e:
                logger.error(f"Error stopping strategy {strategy.name}: {e}")
            self._strategies.remove(strategy)
            logger.info(f"Removed strategy: {strategy.name}")

    def run_forever(self) -> None:
        """
        Run the orchestrator until interrupted.

        Handles SIGINT and SIGTERM for graceful shutdown.
        """
        # Set up signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start and run
        self.start()

        try:
            while self._running:
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt, shutting down...")
        finally:
            self.stop()
