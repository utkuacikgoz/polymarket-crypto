"""
Polymarket CLOB WebSocket connector for real-time market prices.

Streams live prices for multiple active markets and handles
dynamic re-subscription when markets change.
"""

import json
import queue
import time
from threading import Event, Lock, Thread
from typing import Dict, List, Optional, Set, Any

import websocket

from config import PolymarketClobConfig, get_config
from models import MarketPriceTick, MarketSpec, SourceType, current_ts_ms
from pubsub import TOPIC_POLYMARKET_PRICES, TOPIC_MARKET_SPEC, publish, subscribe, Subscription
from connectors.base import BaseConnector


class PolymarketClobWebSocketConnector(BaseConnector):
    """
    WebSocket connector for Polymarket CLOB market channel.
    
    Features:
    - Streams real-time price updates for multiple markets
    - Dynamically re-subscribes when markets change
    - Handles connection lifecycle and reconnection
    - Publishes MarketPriceTick events
    """
    
    CHANNEL_MARKET = "market"
    
    def __init__(self, config: Optional[PolymarketClobConfig] = None):
        """
        Initialize the CLOB WebSocket connector.
        
        Args:
            config: CLOB configuration. If None, loads from environment.
        """
        self.config = config or get_config().clob
        
        super().__init__(
            name="polymarket_clob_ws",
            reconnect_base_delay=self.config.reconnect_delay_base_sec,
            reconnect_max_delay=self.config.reconnect_delay_max_sec
        )
        
        self._ws: Optional[websocket.WebSocket] = None
        # Track multiple markets by market_id
        self._current_markets: Dict[str, MarketSpec] = {}
        self._subscribed_tokens: Set[str] = set()
        
        # Token to market mapping for price association
        self._token_to_market: Dict[str, str] = {}
        
        # Token to outcome side mapping (YES/NO)
        self._token_to_side: Dict[str, str] = {}
        
        # Token to series key mapping (e.g., "BTC-15M")
        self._token_to_series: Dict[str, str] = {}
        
        # Market spec subscription
        self._market_subscription: Optional[Subscription] = None
        
        # Thread for handling market spec updates
        self._market_watcher_thread: Optional[Thread] = None
        self._market_watcher_stop = Event()
        
        # Lock for token subscription changes
        self._subscription_lock = Lock()
        
        # Last prices by token ID
        self._last_prices: Dict[str, float] = {}
        
        # Last logged bid/ask to avoid duplicate logs
        self._last_logged_bbo: Dict[str, tuple] = {}  # token_id -> (bid, ask)
        self._last_logged_time: Dict[str, float] = {}  # token_id -> timestamp (for throttling)
    
    @property
    def current_market(self) -> Optional[MarketSpec]:
        """Get the first subscribed market (for backwards compatibility)."""
        if not self._current_markets:
            return None
        return next(iter(self._current_markets.values()))
    
    @property
    def current_markets(self) -> Dict[str, MarketSpec]:
        """Get all currently subscribed markets."""
        return self._current_markets.copy()
    
    @property
    def subscribed_tokens(self) -> Set[str]:
        """Get set of currently subscribed token IDs."""
        with self._subscription_lock:
            return self._subscribed_tokens.copy()
    
    def add_market(self, market: MarketSpec) -> None:
        """
        Add a market to subscribe to.
        
        Args:
            market: Market specification with token IDs
        """
        with self._subscription_lock:
            if market.market_id in self._current_markets:
                return  # Already subscribed
            
            self._current_markets[market.market_id] = market
            new_tokens = set(market.get_token_ids())
            
            # Track token to market mapping
            for token in new_tokens:
                self._token_to_market[token] = market.market_id
            
            # Track token to outcome side (YES=Up, NO=Down)
            if market.token_yes:
                self._token_to_side[market.token_yes] = "UP"
            if market.token_no:
                self._token_to_side[market.token_no] = "DOWN"
            
            # Track token to series key (e.g., "BTC-15M")
            series_key = market.extra.get("series_key", "Unknown") if market.extra else "Unknown"
            for token in new_tokens:
                self._token_to_series[token] = series_key
            
            self.logger.market_switch(None, market.market_id)
            
            if self._ws and self._connected:
                tokens_to_sub = new_tokens - self._subscribed_tokens
                if tokens_to_sub:
                    self._subscribe_tokens(list(tokens_to_sub))
                    self._subscribed_tokens.update(tokens_to_sub)
    
    def remove_market(self, market_id: str) -> None:
        """
        Remove a market subscription.
        
        Args:
            market_id: Market ID to remove
        """
        with self._subscription_lock:
            if market_id not in self._current_markets:
                return
            
            market = self._current_markets.pop(market_id)
            tokens_to_remove = set(market.get_token_ids())
            
            # Check if any tokens are still needed by other markets
            all_needed_tokens = set()
            for m in self._current_markets.values():
                all_needed_tokens.update(m.get_token_ids())
            
            tokens_to_unsub = tokens_to_remove - all_needed_tokens
            
            # Clean up token mapping
            for token in tokens_to_remove:
                if token in self._token_to_market:
                    del self._token_to_market[token]
            
            if self._ws and self._connected and tokens_to_unsub:
                self._unsubscribe_tokens(list(tokens_to_unsub))
                self._subscribed_tokens -= tokens_to_unsub
    
    def set_market(self, market: MarketSpec) -> None:
        """
        Set the market to subscribe to (backwards compatibility).
        
        Clears existing markets and subscribes to just this one.
        
        Args:
            market: Market specification with token IDs
        """
        self.add_market(market)
    
    def start(self) -> None:
        """Start the connector and market watcher."""
        # Start market spec watcher
        self._market_watcher_stop.clear()
        self._market_subscription = subscribe(TOPIC_MARKET_SPEC, f"{self.name}_market_watcher")
        self._market_watcher_thread = Thread(
            target=self._watch_market_updates,
            name=f"{self.name}-market-watcher",
            daemon=True
        )
        self._market_watcher_thread.start()
        
        # Start main connector
        super().start()
    
    def stop(self, timeout: float = 5.0) -> None:
        """Stop the connector and all related threads."""
        # Stop market watcher
        self._market_watcher_stop.set()
        if self._market_watcher_thread and self._market_watcher_thread.is_alive():
            self._market_watcher_thread.join(timeout=2.0)
        
        # Unsubscribe from market spec
        if self._market_subscription:
            from pubsub import unsubscribe
            unsubscribe(self._market_subscription)
            self._market_subscription = None
        
        # Close WebSocket
        self._close_websocket()
        
        # Stop main thread
        super().stop(timeout)
    
    def _watch_market_updates(self) -> None:
        """
        Background thread that watches for market spec updates.
        
        When a new MarketSpec is published, adds it to subscriptions.
        """
        while not self._market_watcher_stop.is_set():
            try:
                if self._market_subscription:
                    market = self._market_subscription.get(timeout=1.0)
                    if isinstance(market, MarketSpec):
                        self.add_market(market)
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error in market watcher: {e}")
    
    def _connect(self) -> None:
        """
        Establish WebSocket connection and process messages.
        
        Waits for a market to be available before connecting, since
        Polymarket closes connections that don't have active subscriptions.
        """
        # Wait for a market to be available before connecting
        # This prevents idle connections that Polymarket will close
        while not self._should_stop():
            with self._subscription_lock:
                if self._current_markets:
                    break
            self.logger.debug("Waiting for market spec before connecting...")
            if not self._wait(2.0):
                return
        
        url = f"{self.config.ws_base_url}/ws/{self.CHANNEL_MARKET}"
        self.logger.info(f"Connecting to {url}")
        
        self._ws = websocket.create_connection(
            url,
            timeout=30.0,
            enable_multithread=True
        )
        
        try:
            self._set_connected(True)
            self.logger.connected(url)
            
            # Subscribe to all current market tokens
            with self._subscription_lock:
                if self._current_markets:
                    all_tokens = set()
                    for market in self._current_markets.values():
                        all_tokens.update(market.get_token_ids())
                    
                    tokens = list(all_tokens)
                    self._do_initial_subscribe(tokens)
                    self._subscribed_tokens = all_tokens
                    self.logger.info(f"Subscribed to {len(self._current_markets)} markets, {len(tokens)} tokens")
                else:
                    # Markets disappeared while connecting, disconnect and retry
                    self.logger.warning("No market available after connection, will retry")
                    return
            
            last_ping_time = time.time()
            
            while not self._should_stop():
                current_time = time.time()
                
                # Send ping to keep connection alive
                if current_time - last_ping_time >= self.config.ping_interval_sec:
                    try:
                        self._ws.send("PING")
                        last_ping_time = current_time
                        self._update_heartbeat()
                        self.logger.heartbeat()
                    except Exception as e:
                        self.logger.warning(f"Ping failed: {e}")
                        break
                
                self._ws.settimeout(1.0)
                
                try:
                    message = self._ws.recv()
                    
                    if not message:
                        continue
                    
                    # Handle pong response
                    if message == "PONG":
                        continue
                    
                    self._process_message(message)
                    
                except websocket.WebSocketTimeoutException:
                    continue
                except websocket.WebSocketConnectionClosedException:
                    self.logger.warning("WebSocket connection closed")
                    break
                except Exception as e:
                    self.logger.error(f"Error receiving message: {e}")
                    break
                    
        finally:
            self._close_websocket()
    
    def _do_initial_subscribe(self, token_ids: List[str]) -> None:
        """
        Send initial subscription message on connect.
        
        Args:
            token_ids: List of token IDs to subscribe to
        """
        if not token_ids:
            return
        
        subscribe_msg = {
            "assets_ids": token_ids,
            "type": self.CHANNEL_MARKET
        }
        
        self._ws.send(json.dumps(subscribe_msg))
        self.logger.info(f"Subscribed to {len(token_ids)} tokens", token_ids=token_ids)
    
    def _subscribe_tokens(self, token_ids: List[str]) -> None:
        """
        Subscribe to additional token IDs.
        
        Args:
            token_ids: Token IDs to subscribe to
        """
        if not token_ids or not self._ws:
            return
        
        subscribe_msg = {
            "assets_ids": token_ids,
            "operation": "subscribe"
        }
        
        try:
            self._ws.send(json.dumps(subscribe_msg))
            self.logger.info(f"Subscribed to tokens", token_ids=token_ids)
        except Exception as e:
            self.logger.error(f"Failed to subscribe: {e}")
    
    def _unsubscribe_tokens(self, token_ids: List[str]) -> None:
        """
        Unsubscribe from token IDs.
        
        Args:
            token_ids: Token IDs to unsubscribe from
        """
        if not token_ids or not self._ws:
            return
        
        unsubscribe_msg = {
            "assets_ids": token_ids,
            "operation": "unsubscribe"
        }
        
        try:
            self._ws.send(json.dumps(unsubscribe_msg))
            self.logger.info(f"Unsubscribed from tokens", token_ids=token_ids)
        except Exception as e:
            self.logger.error(f"Failed to unsubscribe: {e}")
    
    def _process_message(self, message: str) -> None:
        """
        Process a WebSocket message.
        
        Messages can be price updates, order book changes, etc.
        
        Args:
            message: Raw JSON message
        """
        try:
            data = json.loads(message)
            
            self._update_last_message()
            
            # Handle different message types from Polymarket
            # The market channel sends various event types
            
            if isinstance(data, list):
                # Batch of events
                for event in data:
                    self._process_event(event)
            elif isinstance(data, dict):
                self._process_event(data)
                
        except json.JSONDecodeError as e:
            self.logger.debug(f"Invalid JSON: {message[:100]}")
        except Exception as e:
            self.logger.warning(f"Error processing message: {e}")
    
    def _process_event(self, event: Dict[str, Any]) -> None:
        """
        Process a single event from the WebSocket.
        
        Args:
            event: Event dictionary
        """
        event_type = event.get("event_type", event.get("type", ""))
        
        # Handle price book events
        if event_type in ("price_change", "book", "last_trade_price", "best_bid_ask"):
            self._handle_price_event(event)
        elif event_type == "tick_size":
            # Tick size update - informational
            self.logger.debug(f"Tick size update: {event}")
        else:
            # Log unknown event types for debugging
            self.logger.debug(f"Unknown event type: {event_type}", event=event)
    
    def _handle_price_event(self, event: Dict[str, Any]) -> None:
        """
        Handle price-related events and log best bid/ask.
        
        Polymarket CLOB sends different event formats:
        - 'book': Full order book with 'bids' and 'asks' arrays
        - 'price_change': Incremental price updates with 'price_changes' array
        - 'last_trade_price': Last trade with 'price', 'asset_id'
        
        Args:
            event: Price event data
        """
        event_type = event.get("event_type", "")
        
        try:
            # Handle 'book' event - extract best bid/ask from order book arrays
            if event_type == "book":
                asset_id = event.get("asset_id", "")
                if not asset_id:
                    return
                
                # Get context for logging
                series_key = self._token_to_series.get(asset_id, "Unknown")
                outcome_side = self._token_to_side.get(asset_id, "?")
                market_id = self._token_to_market.get(asset_id, "")
                
                bids = event.get("bids", [])
                asks = event.get("asks", [])
                
                # Best bid is highest price in bids, best ask is lowest price in asks
                # Format: [{"price": "0.85", "size": "1000"}, ...]
                best_bid = None
                best_bid_qty = None
                best_ask = None
                best_ask_qty = None
                
                if bids:
                    # Bids should be sorted highest first
                    best_bid = float(bids[0].get("price", 0))
                    best_bid_qty = float(bids[0].get("size", 0))
                
                if asks:
                    # Asks should be sorted lowest first
                    best_ask = float(asks[0].get("price", 0))
                    best_ask_qty = float(asks[0].get("size", 0))
                
                # Only log if bid/ask PRICE changed (not quantity)
                # Use time-based throttling: log at most once per 60 seconds for unchanged prices
                last_bbo = self._last_logged_bbo.get(asset_id)
                current_bbo = (round(best_bid, 4) if best_bid is not None else None, 
                               round(best_ask, 4) if best_ask is not None else None)
                
                current_time = time.time()
                last_log_time = self._last_logged_time.get(asset_id, 0)
                time_since_last_log = current_time - last_log_time
                
                # Log if: price changed OR it's been 60+ seconds since last log
                price_changed = last_bbo != current_bbo
                should_log = (price_changed or time_since_last_log >= 60.0) and \
                             (best_bid is not None or best_ask is not None)
                
                # DEBUG: Print to stderr to see what's happening
                import sys
                print(f"DEDUP: asset={asset_id[:8]} last={last_bbo} curr={current_bbo} changed={price_changed} time={time_since_last_log:.1f}s should_log={should_log}", file=sys.stderr)
                
                if should_log:
                    self._last_logged_bbo[asset_id] = current_bbo
                    self._last_logged_time[asset_id] = current_time
                    
                    bid_str = f"{best_bid:.4f}" if best_bid else "N/A"
                    ask_str = f"{best_ask:.4f}" if best_ask else "N/A"
                    bid_qty_str = f"({best_bid_qty:.0f})" if best_bid_qty else ""
                    ask_qty_str = f"({best_ask_qty:.0f})" if best_ask_qty else ""
                    
                    self.logger.info(
                        f"[{series_key}] {outcome_side}: Bid={bid_str}{bid_qty_str} Ask={ask_str}{ask_qty_str}",
                        series=series_key,
                        side=outcome_side,
                        best_bid=best_bid,
                        best_ask=best_ask,
                        bid_qty=best_bid_qty,
                        ask_qty=best_ask_qty,
                        token_id=asset_id[:20] + "..."
                    )
                
                # Calculate mid price and publish tick
                if best_bid and best_ask:
                    mid_price = (best_bid + best_ask) / 2
                    tick = MarketPriceTick(
                        ts_ms=current_ts_ms(),
                        market_id=market_id,
                        token_id=asset_id,
                        price=mid_price,
                        side=outcome_side,
                        source=SourceType.POLYMARKET_WS
                    )
                    self._last_prices[asset_id] = mid_price
                    publish(TOPIC_POLYMARKET_PRICES, tick)
                
                # Also use last_trade_price if present
                last_trade = event.get("last_trade_price")
                if last_trade:
                    price = float(last_trade)
                    self._last_prices[asset_id] = price
            
            # Handle 'last_trade_price' event
            elif event_type == "last_trade_price":
                asset_id = event.get("asset_id", "")
                if not asset_id:
                    return
                
                price = event.get("price")
                if price:
                    market_id = self._token_to_market.get(asset_id, "")
                    outcome_side = self._token_to_side.get(asset_id, "?")
                    
                    tick = MarketPriceTick(
                        ts_ms=current_ts_ms(),
                        market_id=market_id,
                        token_id=asset_id,
                        price=float(price),
                        side=outcome_side,
                        source=SourceType.POLYMARKET_WS
                    )
                    self._last_prices[asset_id] = float(price)
                    publish(TOPIC_POLYMARKET_PRICES, tick)
            
            # Handle 'price_change' event - may contain multiple asset updates
            elif event_type == "price_change":
                price_changes = event.get("price_changes", [])
                for change in price_changes:
                    asset_id = change.get("asset_id", "")
                    if not asset_id:
                        continue
                    
                    price = change.get("price")
                    if price:
                        market_id = self._token_to_market.get(asset_id, "")
                        outcome_side = self._token_to_side.get(asset_id, "?")
                        
                        tick = MarketPriceTick(
                            ts_ms=current_ts_ms(),
                            market_id=market_id,
                            token_id=asset_id,
                            price=float(price),
                            side=outcome_side,
                            source=SourceType.POLYMARKET_WS
                        )
                        self._last_prices[asset_id] = float(price)
                        publish(TOPIC_POLYMARKET_PRICES, tick)
                
        except (KeyError, ValueError, TypeError) as e:
            self.logger.debug(f"Error parsing price event: {e}")
    
    def _close_websocket(self) -> None:
        """Close the WebSocket connection."""
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
            finally:
                self._ws = None
                self._subscribed_tokens.clear()
    
    def get_last_price(self, token_id: str) -> Optional[float]:
        """
        Get the last known price for a token.
        
        Args:
            token_id: Token ID
            
        Returns:
            Last price or None if not available
        """
        return self._last_prices.get(token_id)


class PolymarketClobWebSocketConnectorSimple(BaseConnector):
    """
    Simplified CLOB WebSocket connector without automatic market watching.
    
    Requires manual calls to set_market() to change subscriptions.
    Useful when the caller wants full control over market switching.
    """
    
    CHANNEL_MARKET = "market"
    
    def __init__(self, config: Optional[PolymarketClobConfig] = None):
        """
        Initialize the simplified CLOB WebSocket connector.
        
        Args:
            config: CLOB configuration
        """
        self.config = config or get_config().clob
        
        super().__init__(
            name="polymarket_clob_ws_simple",
            reconnect_base_delay=self.config.reconnect_delay_base_sec,
            reconnect_max_delay=self.config.reconnect_delay_max_sec
        )
        
        self._ws: Optional[websocket.WebSocket] = None
        self._token_ids: List[str] = []
        self._market_id: str = ""
        self._lock = Lock()
    
    def set_tokens(self, market_id: str, token_ids: List[str]) -> None:
        """
        Set the tokens to subscribe to.
        
        If already connected, re-subscribes to new tokens.
        
        Args:
            market_id: Market condition ID
            token_ids: List of token IDs
        """
        with self._lock:
            old_tokens = set(self._token_ids)
            new_tokens = set(token_ids)
            
            self._market_id = market_id
            self._token_ids = list(token_ids)
            
            if self._ws and self._connected:
                # Unsubscribe from removed tokens
                removed = old_tokens - new_tokens
                if removed:
                    self._send_unsubscribe(list(removed))
                
                # Subscribe to added tokens
                added = new_tokens - old_tokens
                if added:
                    self._send_subscribe(list(added))
    
    def _connect(self) -> None:
        """Establish connection and process messages."""
        url = f"{self.config.ws_base_url}/ws/{self.CHANNEL_MARKET}"
        
        self._ws = websocket.create_connection(url, timeout=30.0, enable_multithread=True)
        
        try:
            self._set_connected(True)
            
            # Initial subscription
            with self._lock:
                if self._token_ids:
                    msg = {"assets_ids": self._token_ids, "type": self.CHANNEL_MARKET}
                    self._ws.send(json.dumps(msg))
            
            last_ping = time.time()
            
            while not self._should_stop():
                if time.time() - last_ping >= self.config.ping_interval_sec:
                    self._ws.send("PING")
                    last_ping = time.time()
                    self._update_heartbeat()
                
                self._ws.settimeout(1.0)
                
                try:
                    msg = self._ws.recv()
                    if msg and msg != "PONG":
                        self._process_message(msg)
                except websocket.WebSocketTimeoutException:
                    continue
                except websocket.WebSocketConnectionClosedException:
                    break
                    
        finally:
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass
                self._ws = None
    
    def _send_subscribe(self, token_ids: List[str]) -> None:
        """Send subscribe message."""
        if self._ws and token_ids:
            msg = {"assets_ids": token_ids, "operation": "subscribe"}
            self._ws.send(json.dumps(msg))
    
    def _send_unsubscribe(self, token_ids: List[str]) -> None:
        """Send unsubscribe message."""
        if self._ws and token_ids:
            msg = {"assets_ids": token_ids, "operation": "unsubscribe"}
            self._ws.send(json.dumps(msg))
    
    def _process_message(self, message: str) -> None:
        """Process incoming message."""
        try:
            data = json.loads(message)
            self._update_last_message()
            
            events = data if isinstance(data, list) else [data]
            
            for event in events:
                self._handle_event(event)
                
        except json.JSONDecodeError:
            pass
    
    def _handle_event(self, event: Dict[str, Any]) -> None:
        """Handle a single event."""
        asset_id = event.get("asset_id", event.get("token_id", ""))
        price = event.get("price") or event.get("last_trade_price")
        
        if asset_id and price is not None:
            tick = MarketPriceTick(
                ts_ms=current_ts_ms(),
                market_id=self._market_id,
                token_id=asset_id,
                price=float(price),
                side=None,
                source=SourceType.POLYMARKET_WS
            )
            publish(TOPIC_POLYMARKET_PRICES, tick)
    
    def stop(self, timeout: float = 5.0) -> None:
        """Stop the connector."""
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        super().stop(timeout)
