"""
Polymarket RTDS WebSocket connector for Chainlink price feeds.

Streams real-time Chainlink oracle prices for cryptocurrency pairs.
Docs: https://docs.polymarket.com/developers/RTDS/RTDS-crypto-prices
"""

import json
import ssl
import time

import websocket

from config import PolymarketGammaConfig, RTDSConfig, get_config
from connectors.base import BaseConnector
from models import (
    ChainlinkPriceTick,
    RTDSSubscription,
    current_ts_ms,
    parse_rtds_message,
)
from pubsub import TOPIC_CHAINLINK_PRICES, publish


class RTDSChainlinkConnector(BaseConnector):
    """
    WebSocket connector for Polymarket RTDS Chainlink price feeds.

    Streams real-time Chainlink oracle prices and publishes
    ChainlinkPriceTick events for subscribed symbols.

    Features:
    - Subscribe to specific symbols or all available prices
    - Automatic reconnection with exponential backoff
    - Derives symbols from configured target series (e.g., BTC-15M -> btc/usd)
    """

    def __init__(
        self,
        config: RTDSConfig | None = None,
        gamma_config: PolymarketGammaConfig | None = None
    ):
        """
        Initialize the RTDS Chainlink connector.

        Args:
            config: RTDS configuration. If None, loads from environment.
            gamma_config: Gamma config to derive symbols from. If None, loads from environment.
        """
        self.config = config or get_config().rtds
        self._gamma_config = gamma_config or get_config().gamma

        super().__init__(
            name="rtds_chainlink",
            reconnect_base_delay=self.config.reconnect_delay_base_sec,
            reconnect_max_delay=self.config.reconnect_delay_max_sec
        )

        self._ws: websocket.WebSocket | None = None

        # Track last prices per symbol
        self._last_prices: dict[str, ChainlinkPriceTick] = {}

        # Symbols to subscribe to
        self._subscribed_symbols: set[str] = set()

    @property
    def last_price(self) -> ChainlinkPriceTick | None:
        """Get the most recent price tick across all symbols."""
        if not self._last_prices:
            return None
        return max(self._last_prices.values(), key=lambda t: t.ts_ms)

    @property
    def last_prices(self) -> dict[str, ChainlinkPriceTick]:
        """Get the last price for each symbol."""
        return self._last_prices.copy()

    def get_price(self, symbol: str) -> ChainlinkPriceTick | None:
        """Get the last price for a specific symbol."""
        return self._last_prices.get(symbol.lower())

    def _get_symbols_to_subscribe(self) -> tuple:
        """
        Determine which symbols to subscribe to.

        Derives symbols from target_series in PolymarketGammaConfig.
        For example: ("BTC", "15M") -> "btc/usd"

        Returns:
            Tuple of symbol strings (e.g., ("btc/usd", "eth/usd"))
        """
        # Derive from Gamma config target series
        return RTDSConfig.get_chainlink_symbols_for_series(self._gamma_config.target_series)

    def _connect(self) -> None:
        """
        Establish WebSocket connection and process messages.
        """
        url = self.config.ws_base_url
        symbols = self._get_symbols_to_subscribe()

        self.logger.info(
            f"Connecting to RTDS at {url}",
            symbols=list(symbols) if symbols else ["all"]
        )

        # Create SSL context based on config
        sslopt = None
        if not self.config.ssl_verify:
            sslopt = {
                "cert_reqs": ssl.CERT_NONE,
                "check_hostname": False,
            }
            self.logger.warning("SSL verification disabled")

        # Create WebSocket connection
        self._ws = websocket.create_connection(
            url,
            timeout=self.config.ping_interval_sec,
            enable_multithread=True,
            sslopt=sslopt,
        )

        try:
            self._set_connected(True)
            self.logger.connected(url, symbols=list(symbols) if symbols else ["all"])

            # Subscribe to Chainlink prices
            self._subscribe_chainlink(symbols)

            last_ping_time = time.time()

            while not self._should_stop():
                # Check if we need to send a ping
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
                    continue
                except websocket.WebSocketConnectionClosedException:
                    self.logger.warning("RTDS WebSocket connection closed")
                    break
                except Exception as e:
                    self.logger.error(f"Error receiving message: {e}")
                    break
        finally:
            self._close_websocket()

    def _subscribe_chainlink(self, symbols: tuple) -> None:
        """
        Send subscription message for Chainlink prices.

        Args:
            symbols: Tuple of symbols to subscribe to. Empty for all symbols.
        """
        if not self._ws:
            return

        # Subscribe to all Chainlink prices first (filters="")
        # Individual symbol filtering happens client-side
        sub = RTDSSubscription.chainlink_all()
        msg = sub.to_subscribe_message()
        msg_json = json.dumps(msg)

        self.logger.info(f"Sending Chainlink subscription: {msg_json}")
        self._ws.send(msg_json)

        # Track which symbols we want to filter for
        if symbols:
            for symbol in symbols:
                self._subscribed_symbols.add(symbol.lower())
            self.logger.info(f"Subscribed to Chainlink prices (filtering for: {list(symbols)})")
        else:
            self.logger.info("Subscribed to all Chainlink prices")

    def _process_message(self, message: str) -> None:
        """
        Process a WebSocket message.

        Args:
            message: Raw JSON message from WebSocket
        """
        try:
            data = json.loads(message)
            received_ts = current_ts_ms()

            topic = data.get("topic", "unknown")
            msg_type = data.get("type", "unknown")

            # Log all incoming messages for debugging
            self.logger.debug(f"RTDS message: topic={topic}, type={msg_type}")

            # Parse the RTDS message (only handles crypto_prices_chainlink + update)
            tick = parse_rtds_message(data, received_ts)

            if tick:
                # Filter by subscribed symbols if configured
                if self._subscribed_symbols and tick.symbol.lower() not in self._subscribed_symbols:
                    return  # Skip symbols we're not interested in

                # Store by symbol
                self._last_prices[tick.symbol.lower()] = tick
                self._update_last_message()
                self._update_heartbeat()

                # Publish the tick
                publish(TOPIC_CHAINLINK_PRICES, tick)

                # Log the price update
                self.logger.tick(
                    tick.to_dict(),
                    symbol=tick.symbol,
                    price=tick.price,
                    base=tick.base_currency
                )
            else:
                # Log non-update messages (subscription confirmations, etc.)
                if topic == "crypto_prices_chainlink":
                    if msg_type == "subscribe":
                        self.logger.info("Chainlink subscription confirmed")
                    else:
                        self.logger.debug(f"Chainlink message type={msg_type}")
                else:
                    self.logger.debug(f"Non-Chainlink message: topic={topic}, type={msg_type}")

        except json.JSONDecodeError as e:
            self.logger.warning(f"Invalid JSON message: {e}")
        except Exception as e:
            self.logger.warning(f"Error processing message: {e}", exc_info=True)

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
        self._close_websocket()
        super().stop(timeout)
