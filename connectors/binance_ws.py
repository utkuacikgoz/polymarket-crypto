"""
Binance WebSocket connector for bookTicker streams.

Streams best bid/ask prices for multiple symbols and publishes mid-price ticks.
"""

import json
import ssl
import time

import websocket

from config import BinanceConfig, get_config
from connectors.base import BaseConnector
from models import PriceTick
from pubsub import TOPIC_BINANCE_TICKS, publish


class BinanceWebSocketConnector(BaseConnector):
    """
    WebSocket connector for Binance Spot bookTicker streams.

    Streams real-time best bid/ask updates for multiple symbols
    and publishes PriceTick events with the calculated mid-price.

    Uses the combined stream endpoint to subscribe to multiple symbols
    in a single connection.
    """

    def __init__(self, config: BinanceConfig | None = None):
        """
        Initialize the Binance WebSocket connector.

        Args:
            config: Binance configuration. If None, loads from environment.
        """
        self.config = config or get_config().binance

        super().__init__(
            name="binance_ws",
            reconnect_base_delay=self.config.reconnect_delay_base_sec,
            reconnect_max_delay=self.config.reconnect_delay_max_sec
        )

        self._ws: websocket.WebSocket | None = None
        # Track last tick per symbol
        self._last_ticks: dict[str, PriceTick] = {}

    @property
    def last_tick(self) -> PriceTick | None:
        """Get the most recent tick across all symbols."""
        if not self._last_ticks:
            return None
        # Return the most recent tick by timestamp
        return max(self._last_ticks.values(), key=lambda t: t.ts_ms)

    @property
    def last_ticks(self) -> dict[str, PriceTick]:
        """Get the last tick for each symbol."""
        return self._last_ticks.copy()

    def get_tick(self, symbol: str) -> PriceTick | None:
        """Get the last tick for a specific symbol."""
        return self._last_ticks.get(symbol.upper())

    def _connect(self) -> None:
        """
        Establish WebSocket connection and process messages.

        Uses combined stream endpoint for multiple symbols.
        """
        url = self.config.stream_url
        self.logger.info(f"Connecting to {url}", symbols=list(self.config.symbols))

        # Create SSL context based on config
        sslopt = None
        if not self.config.ssl_verify:
            sslopt = {
                "cert_reqs": ssl.CERT_NONE,
                "check_hostname": False,
            }
            self.logger.warning("SSL verification disabled")

        # Create WebSocket connection with ping/pong handling
        self._ws = websocket.create_connection(
            url,
            timeout=self.config.ping_timeout_sec,
            enable_multithread=True,
            sslopt=sslopt,
        )

        try:
            self._set_connected(True)
            self.logger.connected(url, symbols=list(self.config.symbols))

            last_ping_time = time.time()

            while not self._should_stop():
                # Check if we need to send a ping (keep-alive)
                current_time = time.time()
                if current_time - last_ping_time >= self.config.ping_interval_sec:
                    try:
                        self._ws.ping()
                        last_ping_time = current_time
                        self._update_heartbeat()
                        self.logger.heartbeat()
                    except Exception as e:
                        self.logger.warning(f"Ping failed: {e}")
                        break

                # Set a short timeout to allow checking stop event
                self._ws.settimeout(1.0)

                try:
                    message = self._ws.recv()

                    if not message:
                        continue

                    # Handle pong frames (websocket-client handles this internally)
                    if isinstance(message, bytes):
                        continue

                    self._process_message(message)

                except websocket.WebSocketTimeoutException:
                    # Timeout is normal, just continue
                    continue
                except websocket.WebSocketConnectionClosedException:
                    self.logger.warning("WebSocket connection closed")
                    break
                except Exception as e:
                    self.logger.error(f"Error receiving message: {e}")
                    break
        finally:
            self._close_websocket()

    def _process_message(self, message: str) -> None:
        """
        Process a WebSocket message.

        Handles combined stream format: {"stream": "btcusdt@bookTicker", "data": {...}}

        Args:
            message: Raw JSON message from WebSocket
        """
        try:
            data = json.loads(message)

            # Combined stream format has "stream" and "data" fields
            if "stream" in data and "data" in data:
                ticker_data = data["data"]
            else:
                # Direct stream format (single symbol)
                ticker_data = data

            # Validate bookTicker message
            if "s" not in ticker_data or "b" not in ticker_data or "a" not in ticker_data:
                self.logger.debug(f"Ignoring non-bookTicker message: {message[:100]}")
                return

            # Create PriceTick from the message
            tick = PriceTick.from_binance_book_ticker(ticker_data)

            # Store by symbol (uppercase)
            self._last_ticks[tick.symbol.upper()] = tick
            self._update_last_message()
            self._update_heartbeat()

            # Publish the tick
            publish(TOPIC_BINANCE_TICKS, tick)

            self.logger.tick(tick.to_dict(), symbol=tick.symbol, mid=tick.mid, bid=tick.bid, ask=tick.ask)

        except json.JSONDecodeError as e:
            self.logger.warning(f"Invalid JSON message: {e}")
        except (KeyError, ValueError) as e:
            self.logger.warning(f"Error parsing message: {e}")

    def _close_websocket(self) -> None:
        """Close the WebSocket connection gracefully."""
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                self.logger.debug(f"Error closing WebSocket: {e}")
            finally:
                self._ws = None

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the connector and close WebSocket."""
        # Close WebSocket first to unblock recv()
        self._close_websocket()
        super().stop(timeout)


class BinanceSubscriptionConnector(BaseConnector):
    """
    Alternative Binance connector using subscription-based messaging.

    This connector uses the subscription API to subscribe to streams
    after connecting, which allows dynamic subscription management.
    Supports multiple symbols.
    """

    def __init__(self, config: BinanceConfig | None = None):
        """
        Initialize the Binance subscription connector.

        Args:
            config: Binance configuration. If None, loads from environment.
        """
        self.config = config or get_config().binance

        super().__init__(
            name="binance_ws_sub",
            reconnect_base_delay=self.config.reconnect_delay_base_sec,
            reconnect_max_delay=self.config.reconnect_delay_max_sec
        )

        self._ws: websocket.WebSocket | None = None
        self._last_ticks: dict[str, PriceTick] = {}
        self._request_id = 0

    @property
    def last_tick(self) -> PriceTick | None:
        """Get the most recent tick across all symbols."""
        if not self._last_ticks:
            return None
        return max(self._last_ticks.values(), key=lambda t: t.ts_ms)

    @property
    def last_ticks(self) -> dict[str, PriceTick]:
        """Get the last tick for each symbol."""
        return self._last_ticks.copy()

    def _get_next_request_id(self) -> int:
        """Get the next request ID for WebSocket messages."""
        self._request_id += 1
        return self._request_id

    def _connect(self) -> None:
        """Connect and subscribe to bookTicker streams for all symbols."""
        # Connect to base endpoint
        url = f"{self.config.ws_base_url}/ws"
        self.logger.info(f"Connecting to {url}", symbols=list(self.config.symbols))

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
            timeout=self.config.ping_timeout_sec,
            enable_multithread=True,
            sslopt=sslopt,
        )

        try:
            self._set_connected(True)

            # Subscribe to bookTicker streams for all symbols
            stream_names = [f"{s.lower()}@bookTicker" for s in self.config.symbols]
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": stream_names,
                "id": self._get_next_request_id()
            }
            self._ws.send(json.dumps(subscribe_msg))
            self.logger.info(f"Subscribed to {len(stream_names)} streams", streams=stream_names)

            last_ping_time = time.time()

            while not self._should_stop():
                current_time = time.time()
                if current_time - last_ping_time >= self.config.ping_interval_sec:
                    try:
                        self._ws.ping()
                        last_ping_time = current_time
                        self._update_heartbeat()
                    except Exception:
                        break

                self._ws.settimeout(1.0)

                try:
                    message = self._ws.recv()
                    if message and isinstance(message, str):
                        self._process_message(message)
                except websocket.WebSocketTimeoutException:
                    continue
                except websocket.WebSocketConnectionClosedException:
                    break
                except Exception as e:
                    self.logger.error(f"Error: {e}")
                    break
        finally:
            self._close_websocket()

    def _process_message(self, message: str) -> None:
        """Process a WebSocket message."""
        try:
            data = json.loads(message)

            # Skip subscription confirmation messages
            if "result" in data:
                self.logger.debug(f"Subscription response: {data}")
                return

            # Handle bookTicker data
            if "s" in data and "b" in data and "a" in data:
                tick = PriceTick.from_binance_book_ticker(data)
                self._last_ticks[tick.symbol.upper()] = tick
                self._update_last_message()
                self._update_heartbeat()
                publish(TOPIC_BINANCE_TICKS, tick)
                self.logger.tick(tick.to_dict())

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            self.logger.warning(f"Error processing message: {e}")

    def _close_websocket(self) -> None:
        """Close WebSocket connection."""
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
            finally:
                self._ws = None

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the connector."""
        self._close_websocket()
        super().stop(timeout)
