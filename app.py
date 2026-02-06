#!/usr/bin/env python3
"""
Main application orchestrator for market data connectors.

Wires together all connectors, manages health monitoring,
and provides a unified interface for running the system.
"""

import signal
import sys
import time
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from typing import Dict, List, Optional, Any

from config import AppConfig, get_config
from models import (
    ConnectorHealth, HealthEvent, MarketSpec, PriceTick, 
    MarketPriceTick, current_ts_ms
)
from pubsub import (
    TOPIC_BINANCE_TICKS, TOPIC_MARKET_SPEC, TOPIC_POLYMARKET_PRICES, TOPIC_HEALTH,
    get_event_bus, subscribe, Subscription
)
from logging_utils import setup_logging, get_logger

from connectors.binance_ws import BinanceWebSocketConnector
from connectors.polymarket_gamma import PolymarketGammaConnector
from connectors.polymarket_clob_ws import PolymarketClobWebSocketConnector
from connectors.polymarket_clob_rest import PolymarketClobRestConnector
from connectors.base import BaseConnector


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
    
    def __init__(self, config: Optional[AppConfig] = None):
        """
        Initialize the orchestrator.
        
        Args:
            config: Application configuration. If None, loads from environment.
        """
        self.config = config or get_config()
        
        # Connectors
        self._binance_ws: Optional[BinanceWebSocketConnector] = None
        self._gamma: Optional[PolymarketGammaConnector] = None
        self._clob_ws: Optional[PolymarketClobWebSocketConnector] = None
        self._clob_rest: Optional[PolymarketClobRestConnector] = None
        
        # State
        self._running = False
        self._stop_event = Event()
        self._lock = Lock()
        
        # Health tracking
        self._rest_fallback_active = False
        self._ws_unhealthy_since: Optional[float] = None
        
        # Subscriptions for monitoring
        self._health_subscription: Optional[Subscription] = None
        self._binance_subscription: Optional[Subscription] = None
        self._polymarket_subscription: Optional[Subscription] = None
        self._market_subscription: Optional[Subscription] = None
        
        # Latest data for status display (multi-symbol/multi-market)
        self._last_binance_ticks: Dict[str, PriceTick] = {}  # By symbol
        self._last_polymarket_ticks: Dict[str, MarketPriceTick] = {}  # By token
        self._current_markets: Dict[str, MarketSpec] = {}  # By market_id
        
        # Background threads
        self._status_thread: Optional[Thread] = None
        self._health_monitor_thread: Optional[Thread] = None
        self._data_collector_thread: Optional[Thread] = None
    
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
        
        # Subscribe to event topics for monitoring
        self._health_subscription = subscribe(TOPIC_HEALTH, "orchestrator_health")
        self._binance_subscription = subscribe(TOPIC_BINANCE_TICKS, "orchestrator_binance")
        self._polymarket_subscription = subscribe(TOPIC_POLYMARKET_PRICES, "orchestrator_pm_prices")
        self._market_subscription = subscribe(TOPIC_MARKET_SPEC, "orchestrator_market")
        
        # Start connectors
        logger.info("Starting Binance WebSocket connector")
        self._binance_ws.start()
        
        logger.info("Starting Polymarket Gamma connector")
        self._gamma.start()
        
        logger.info("Starting Polymarket CLOB WebSocket connector")
        self._clob_ws.start()
        
        # REST connector is started on-demand when WS is unhealthy
        # self._clob_rest.start()
        
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
        
        # Stop background threads
        self._stop_background_threads(timeout / 2)
        
        # Stop connectors
        connectors = [
            self._clob_rest,
            self._clob_ws,
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
            self._market_subscription
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
        while not self._stop_event.is_set():
            try:
                # Collect binance ticks (multi-symbol)
                if self._binance_subscription:
                    ticks = self._binance_subscription.get_all()
                    for tick in ticks:
                        if isinstance(tick, PriceTick):
                            self._last_binance_ticks[tick.symbol.upper()] = tick
                
                # Collect polymarket ticks
                if self._polymarket_subscription:
                    ticks = self._polymarket_subscription.get_all()
                    for tick in ticks:
                        if isinstance(tick, MarketPriceTick):
                            self._last_polymarket_ticks[tick.token_id] = tick
                
                # Collect market updates (multi-market)
                if self._market_subscription:
                    markets = self._market_subscription.get_all()
                    for market in markets:
                        if isinstance(market, MarketSpec):
                            self._current_markets[market.market_id] = market
                            # Update REST connector with new market if in fallback mode
                            if self._clob_rest and self._rest_fallback_active:
                                self._clob_rest.add_market(market)
                
            except Exception as e:
                logger.debug(f"Error collecting data: {e}")
            
            time.sleep(0.1)  # Small sleep to prevent tight loop
    
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
        """Print a status line to console."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        # Market info (show count and series)
        market_count = len(self._current_markets)
        if market_count > 0:
            # Show series keys from extra data
            series_keys = set()
            for market in self._current_markets.values():
                if market.extra and "series_key" in market.extra:
                    series_keys.add(market.extra["series_key"])
            series_str = ",".join(sorted(series_keys)) if series_keys else "Unknown"
            
            first_market = next(iter(self._current_markets.values()))
            expiry_dt = datetime.fromtimestamp(first_market.expiry_ts_ms / 1000, tz=timezone.utc)
            expiry = expiry_dt.strftime("%H:%M:%S")
            market_info = f"{market_count} ({series_str})"
        else:
            market_info = "None"
            expiry = "N/A"
        
        # Binance prices (multi-symbol)
        binance_info = []
        for symbol, tick in sorted(self._last_binance_ticks.items()):
            age_ms = current_ts_ms() - tick.ts_ms
            symbol_short = symbol.replace("USDT", "")
            binance_info.append(f"{symbol_short}:${tick.mid:,.0f}({age_ms}ms)")
        binance_str = " | ".join(binance_info) if binance_info else "N/A"
        
        # Polymarket prices (show count)
        pm_count = len(self._last_polymarket_ticks)
        if self._last_polymarket_ticks:
            latest = max(self._last_polymarket_ticks.values(), key=lambda t: t.ts_ms)
            age_ms = current_ts_ms() - latest.ts_ms
            pm_str = f"{pm_count} tokens ({age_ms}ms ago)"
        else:
            pm_str = "N/A"
        
        # Health summary
        health = self._get_health_summary()
        
        # Fallback status
        fallback = " [REST FALLBACK]" if self._rest_fallback_active else ""
        
        status_line = (
            f"[{now}] "
            f"Markets: {market_info} | "
            f"Binance: {binance_str} | "
            f"PM: {pm_str} | "
            f"Health: {health}{fallback}"
        )
        
        print(status_line)
    
    def _get_health_summary(self) -> str:
        """Get a short health summary string."""
        statuses = []
        
        connectors = [
            ("BIN", self._binance_ws),
            ("GAM", self._gamma),
            ("WS", self._clob_ws),
            ("REST", self._clob_rest if self._rest_fallback_active else None),
        ]
        
        for name, connector in connectors:
            if connector:
                status = "✓" if connector.is_healthy() else "✗"
                statuses.append(f"{name}:{status}")
        
        return " ".join(statuses)
    
    def get_all_health(self) -> Dict[str, ConnectorHealth]:
        """Get health status for all connectors."""
        health = {}
        
        connectors = [
            self._binance_ws,
            self._gamma,
            self._clob_ws,
            self._clob_rest
        ]
        
        for connector in connectors:
            if connector:
                health[connector.name] = connector.get_health()
        
        return health
    
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


def main():
    """Main entry point."""
    # Load configuration
    config = AppConfig.from_env()
    
    # Create and run orchestrator
    orchestrator = ConnectorOrchestrator(config)
    orchestrator.run_forever()


if __name__ == "__main__":
    main()
