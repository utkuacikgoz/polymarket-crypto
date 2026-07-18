"""
Polymarket CLOB WebSocket connector for real-time market prices.

Streams live prices for multiple active markets and handles
dynamic re-subscription when markets change.
"""

import json
import queue
import ssl
import time
from threading import Event, Lock, Thread
from typing import Any

import websocket

from config import PolymarketClobConfig, get_config
from connectors.base import BaseConnector
from models import MarketPriceTick, MarketSpec, SourceType, current_ts_ms
from models.polymarket_ws import (
    BestBidAskMessage,
    BookMessage,
    LastTradePriceMessage,
    MarketResolvedMessage,
    NewMarketMessage,
    PolymarketWSMessage,
    PriceChangeMessage,
    TickSizeChangeMessage,
    parse_ws_messages,
)
from pubsub import (
    TOPIC_MARKET_EXPIRED,
    TOPIC_MARKET_SPEC,
    TOPIC_POLYMARKET_PRICES,
    Subscription,
    publish,
    subscribe,
)


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

    def __init__(self, config: PolymarketClobConfig | None = None):
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

        self._ws: websocket.WebSocket | None = None
        # Track multiple markets by market_id
        self._current_markets: dict[str, MarketSpec] = {}
        self._subscribed_tokens: set[str] = set()

        # Token to market mapping for price association
        self._token_to_market: dict[str, str] = {}

        # Token to outcome side mapping (YES/NO)
        self._token_to_side: dict[str, str] = {}

        # Token to series key mapping (e.g., "BTC-15M")
        self._token_to_series: dict[str, str] = {}

        # Market spec subscription (for new markets)
        self._market_subscription: Subscription | None = None

        # Expired market subscription (for cleanup)
        self._expired_subscription: Subscription | None = None

        # Thread for handling market spec updates
        self._market_watcher_thread: Thread | None = None
        self._market_watcher_stop = Event()

        # Lock for token subscription changes
        self._subscription_lock = Lock()

        # Last prices by token ID
        self._last_prices: dict[str, float] = {}

        # Last logged bid/ask to avoid duplicate logs
        self._last_logged_bbo: dict[str, tuple] = {}  # token_id -> (bid, ask)

    @property
    def current_market(self) -> MarketSpec | None:
        """Get the first subscribed market (for backwards compatibility)."""
        if not self._current_markets:
            return None
        return next(iter(self._current_markets.values()))

    @property
    def current_markets(self) -> dict[str, MarketSpec]:
        """Get all currently subscribed markets."""
        return self._current_markets.copy()

    @property
    def subscribed_tokens(self) -> set[str]:
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

            # Clean up all token mappings for removed tokens
            for token in tokens_to_remove:
                if token in self._token_to_market:
                    del self._token_to_market[token]
                if token in self._token_to_side:
                    del self._token_to_side[token]
                if token in self._token_to_series:
                    del self._token_to_series[token]
                if token in self._last_prices:
                    del self._last_prices[token]
                if token in self._last_logged_bbo:
                    del self._last_logged_bbo[token]

            if self._ws and self._connected and tokens_to_unsub:
                self._unsubscribe_tokens(list(tokens_to_unsub))
                self._subscribed_tokens -= tokens_to_unsub
                self.logger.info(
                    "Unsubscribed from expired market tokens",
                    market_id=market_id[:16] + "...",
                    tokens_removed=len(tokens_to_unsub)
                )

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
        self._expired_subscription = subscribe(TOPIC_MARKET_EXPIRED, f"{self.name}_expired_watcher")
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

        # Unsubscribe from market spec and expired markets
        if self._market_subscription:
            from pubsub import unsubscribe
            unsubscribe(self._market_subscription)
            self._market_subscription = None

        if self._expired_subscription:
            from pubsub import unsubscribe
            unsubscribe(self._expired_subscription)
            self._expired_subscription = None

        # Close WebSocket
        self._close_websocket()

        # Stop main thread
        super().stop(timeout)

    def _watch_market_updates(self) -> None:
        """
        Background thread that watches for market spec updates.

        When a new MarketSpec is published, adds it to subscriptions.
        When a MarketSpec expires, removes it from subscriptions.
        """
        while not self._market_watcher_stop.is_set():
            try:
                # Check for new markets
                if self._market_subscription:
                    try:
                        market = self._market_subscription.get(timeout=0.1)
                        if isinstance(market, MarketSpec):
                            self.add_market(market)
                    except queue.Empty:
                        pass

                # Check for expired markets
                if self._expired_subscription:
                    try:
                        expired_market = self._expired_subscription.get(timeout=0.1)
                        if isinstance(expired_market, MarketSpec):
                            self.logger.info(
                                f"Removing expired market: {expired_market.market_id[:16]}...",
                                event_id=expired_market.event_id,
                                market_id=expired_market.market_id
                            )
                            self.remove_market(expired_market.market_id)
                    except queue.Empty:
                        pass

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

        # Create SSL context based on config
        sslopt = None
        if not self.config.ssl_verify:
            sslopt = {
                "cert_reqs": ssl.CERT_NONE,
                "check_hostname": False,
            }
            self.logger.warning("SSL verification disabled")

        self._ws = websocket.create_connection(
            url,
            timeout=30.0,
            enable_multithread=True,
            sslopt=sslopt,
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

    def _do_initial_subscribe(self, token_ids: list[str]) -> None:
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

    def _subscribe_tokens(self, token_ids: list[str]) -> None:
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
            self.logger.info("Subscribed to tokens", token_ids=token_ids)
        except Exception as e:
            self.logger.error(f"Failed to subscribe: {e}")

    def _unsubscribe_tokens(self, token_ids: list[str]) -> None:
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
            self.logger.info("Unsubscribed from tokens", token_ids=token_ids)
        except Exception as e:
            self.logger.error(f"Failed to unsubscribe: {e}")

    def _process_message(self, message: str) -> None:
        """
        Process a WebSocket message.

        Messages can be price updates, order book changes, etc.
        Uses typed message models for safe parsing.

        Args:
            message: Raw JSON message
        """
        self._update_last_message()

        # Parse using typed models
        parsed_messages = parse_ws_messages(message)

        for msg in parsed_messages:
            self._handle_typed_message(msg)

        # Fallback for unknown message types - parse raw JSON
        if not parsed_messages:
            try:
                data = json.loads(message)
                if isinstance(data, list):
                    for event in data:
                        self._process_raw_event(event)
                elif isinstance(data, dict):
                    self._process_raw_event(data)
            except json.JSONDecodeError:
                self.logger.debug(f"Invalid JSON: {message[:100]}")
            except Exception as e:
                self.logger.warning(f"Error processing message: {e}")

    def _handle_typed_message(self, msg: PolymarketWSMessage) -> None:
        """
        Handle a typed WebSocket message.

        Args:
            msg: Parsed typed message object
        """
        if isinstance(msg, BookMessage):
            self._handle_book_message(msg)
        elif isinstance(msg, PriceChangeMessage):
            self._handle_price_change_message(msg)
        elif isinstance(msg, LastTradePriceMessage):
            self._handle_last_trade_message(msg)
        elif isinstance(msg, BestBidAskMessage):
            self._handle_best_bid_ask_message(msg)
        elif isinstance(msg, TickSizeChangeMessage):
            self._handle_tick_size_message(msg)
        elif isinstance(msg, NewMarketMessage):
            self._handle_new_market_message(msg)
        elif isinstance(msg, MarketResolvedMessage):
            self._handle_market_resolved_message(msg)

    def _process_raw_event(self, event: dict[str, Any]) -> None:
        """
        Process a raw event that wasn't parsed into a typed model.
        Fallback for unknown or malformed messages.

        Args:
            event: Event dictionary
        """
        event_type = event.get("event_type", event.get("type", ""))

        if event_type:
            # Log unknown event types for debugging
            self.logger.debug(f"Unhandled event type: {event_type}", event=event)
        # else: empty event type, likely a heartbeat or ack

    def _handle_book_message(self, msg: BookMessage) -> None:
        """
        Handle a full order book snapshot message.

        Args:
            msg: Parsed BookMessage object
        """
        asset_id = msg.asset_id
        if not asset_id:
            return

        # Get context for logging
        self._token_to_series.get(asset_id, "Unknown")
        outcome_side = self._token_to_side.get(asset_id, "?")
        market_id = self._token_to_market.get(asset_id, msg.market)

        best_bid = msg.best_bid_price
        best_ask = msg.best_ask_price

        # Only log if bid/ask price changed (rounded to avoid float noise)
        last_bbo = self._last_logged_bbo.get(asset_id)
        current_bbo = (round(best_bid, 4) if best_bid is not None else None,
                       round(best_ask, 4) if best_ask is not None else None)

        if last_bbo != current_bbo and (best_bid is not None or best_ask is not None):
            self._last_logged_bbo[asset_id] = current_bbo


            # self.logger.info(
            #     f"[{series_key}] {outcome_side}: Bid={bid_str}{bid_qty_str} Ask={ask_str}{ask_qty_str}",
            #     series=series_key,
            #     side=outcome_side,
            #     best_bid=best_bid,
            #     best_ask=best_ask,
            #     bid_qty=best_bid_qty,
            #     ask_qty=best_ask_qty,
            #     token_id=asset_id[:20] + "..."
            # )

        # Calculate mid price and publish tick
        mid_price = msg.mid_price
        if mid_price is not None:
            tick = MarketPriceTick(
                ts_ms=msg.ts_ms,
                market_id=market_id,
                token_id=asset_id,
                price=mid_price,
                side=outcome_side,
                source=SourceType.POLYMARKET_WS
            )
            self._last_prices[asset_id] = mid_price
            publish(TOPIC_POLYMARKET_PRICES, tick)

    def _handle_price_change_message(self, msg: PriceChangeMessage) -> None:
        """
        Handle incremental price change message.

        Args:
            msg: Parsed PriceChangeMessage object
        """
        for change in msg.price_changes:
            asset_id = change.asset_id
            if not asset_id:
                continue

            market_id = self._token_to_market.get(asset_id, msg.market)
            outcome_side = self._token_to_side.get(asset_id, "?")
            self._token_to_series.get(asset_id, "Unknown")

            # Log the price change
            # side_str = "Bid" if change.is_bid else "Ask" if change.is_ask else "?"
            # size_str = f"({change.size_float:.0f})" if change.size else ""
            # self.logger.info(
            #     f"[{series_key}] {outcome_side}: {side_str}={change.price_float:.4f}{size_str}",
            #     series=series_key,
            #     side=outcome_side,
            #     price_side=change.side,
            #     price=change.price_float,
            #     size=change.size_float if change.size else None,
            #     token_id=asset_id[:20] + "..."
            # )

            # Update BBO tracking
            best_bid, best_ask = change.best_bid_float, change.best_ask_float
            if best_bid is not None or best_ask is not None:
                current_bbo = (round(best_bid, 4) if best_bid else None,
                               round(best_ask, 4) if best_ask else None)
                self._last_logged_bbo[asset_id] = current_bbo

            # Calculate mid price if we have both bid and ask
            if best_bid is not None and best_ask is not None:
                mid_price = (best_bid + best_ask) / 2
            else:
                mid_price = change.price_float

            tick = MarketPriceTick(
                ts_ms=msg.ts_ms,
                market_id=market_id,
                token_id=asset_id,
                price=mid_price,
                side=outcome_side,
                source=SourceType.POLYMARKET_WS
            )
            self._last_prices[asset_id] = mid_price
            publish(TOPIC_POLYMARKET_PRICES, tick)

    def _handle_last_trade_message(self, msg: LastTradePriceMessage) -> None:
        """
        Handle last trade price message.

        Args:
            msg: Parsed LastTradePriceMessage object
        """
        asset_id = msg.asset_id
        if not asset_id:
            return

        market_id = self._token_to_market.get(asset_id, msg.market)
        outcome_side = self._token_to_side.get(asset_id, "?")
        self._token_to_series.get(asset_id, "Unknown")

        # Trade logging moved to data_collector_loop in app.py

        tick = MarketPriceTick(
            ts_ms=msg.ts_ms,
            market_id=market_id,
            token_id=asset_id,
            price=msg.price_float,
            side=outcome_side,
            source=SourceType.POLYMARKET_WS
        )
        self._last_prices[asset_id] = msg.price_float
        publish(TOPIC_POLYMARKET_PRICES, tick)

    def _handle_best_bid_ask_message(self, msg: BestBidAskMessage) -> None:
        """
        Handle best bid/ask update message.

        Args:
            msg: Parsed BestBidAskMessage object
        """
        asset_id = msg.asset_id
        if not asset_id:
            return

        market_id = self._token_to_market.get(asset_id, msg.market)
        outcome_side = self._token_to_side.get(asset_id, "?")
        self._token_to_series.get(asset_id, "Unknown")

        # Update BBO tracking
        current_bbo = (round(msg.best_bid_float, 4), round(msg.best_ask_float, 4))
        last_bbo = self._last_logged_bbo.get(asset_id)

        if last_bbo != current_bbo:
            self._last_logged_bbo[asset_id] = current_bbo

            # self.logger.info(
            #     f"[{series_key}] {outcome_side}: BBO Bid={msg.best_bid_float:.4f} Ask={msg.best_ask_float:.4f} Spread={msg.spread_float:.4f}",
            #     series=series_key,
            #     side=outcome_side,
            #     best_bid=msg.best_bid_float,
            #     best_ask=msg.best_ask_float,
            #     spread=msg.spread_float,
            #     token_id=asset_id[:20] + "..."
            # )

        mid_price = msg.mid_price
        tick = MarketPriceTick(
            ts_ms=msg.ts_ms,
            market_id=market_id,
            token_id=asset_id,
            price=mid_price,
            side=outcome_side,
            source=SourceType.POLYMARKET_WS
        )
        self._last_prices[asset_id] = mid_price
        publish(TOPIC_POLYMARKET_PRICES, tick)

    def _handle_tick_size_message(self, msg: TickSizeChangeMessage) -> None:
        """
        Handle tick size change message.

        Args:
            msg: Parsed TickSizeChangeMessage object
        """
        asset_id = msg.asset_id
        series_key = self._token_to_series.get(asset_id, "Unknown")

        self.logger.info(
            f"[{series_key}] Tick size change: {msg.old_tick_size} -> {msg.new_tick_size}",
            series=series_key,
            asset_id=asset_id[:20] + "...",
            old_tick_size=msg.old_tick_size,
            new_tick_size=msg.new_tick_size,
        )

    def _handle_new_market_message(self, msg: NewMarketMessage) -> None:
        """
        Handle new market creation message.

        Args:
            msg: Parsed NewMarketMessage object
        """
        self.logger.info(
            f"New market: {msg.question[:50]}...",
            market_id=msg.market[:20] + "...",
            slug=msg.slug,
            outcomes=list(msg.outcomes),
            assets=list(msg.assets_ids),
        )

    def _handle_market_resolved_message(self, msg: MarketResolvedMessage) -> None:
        """
        Handle market resolution message.

        Args:
            msg: Parsed MarketResolvedMessage object
        """
        self.logger.info(
            f"Market resolved: {msg.question[:50]}... -> {msg.winning_outcome}",
            market_id=msg.market[:20] + "...",
            winning_outcome=msg.winning_outcome,
            winning_asset_id=msg.winning_asset_id[:20] + "..." if msg.winning_asset_id else None,
        )

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

    def get_last_price(self, token_id: str) -> float | None:
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

    def __init__(self, config: PolymarketClobConfig | None = None):
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

        self._ws: websocket.WebSocket | None = None
        self._token_ids: list[str] = []
        self._market_id: str = ""
        self._lock = Lock()

    def set_tokens(self, market_id: str, token_ids: list[str]) -> None:
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

        # Create SSL context based on config
        sslopt = None
        if not self.config.ssl_verify:
            sslopt = {
                "cert_reqs": ssl.CERT_NONE,
                "check_hostname": False,
            }
            self.logger.warning("SSL verification disabled")

        self._ws = websocket.create_connection(
            url,
            timeout=30.0,
            enable_multithread=True,
            sslopt=sslopt,
        )

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

    def _send_subscribe(self, token_ids: list[str]) -> None:
        """Send subscribe message."""
        if self._ws and token_ids:
            msg = {"assets_ids": token_ids, "operation": "subscribe"}
            self._ws.send(json.dumps(msg))

    def _send_unsubscribe(self, token_ids: list[str]) -> None:
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

    def _handle_event(self, event: dict[str, Any]) -> None:
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
