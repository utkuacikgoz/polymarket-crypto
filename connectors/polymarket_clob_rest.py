"""
Polymarket CLOB REST API connector for price polling.

Provides REST-based price fetching as a fallback when WebSocket is unhealthy.
Supports multiple markets simultaneously.
"""

import time
from typing import Dict, List, Optional, Any, Set

import requests

from config import PolymarketClobConfig, get_config
from models import MarketPriceTick, MarketSpec, SourceType, current_ts_ms
from pubsub import TOPIC_POLYMARKET_PRICES, publish
from connectors.base import BaseConnector


class PolymarketClobRestConnector(BaseConnector):
    """
    REST polling connector for Polymarket CLOB API.
    
    Used as a fallback when the WebSocket connector is unhealthy.
    Polls price data for multiple markets at a regular interval.
    
    Features:
    - Multi-market support
    - Configurable polling interval
    - Token subscription management
    - Graceful degradation from WebSocket
    """
    
    def __init__(self, config: Optional[PolymarketClobConfig] = None):
        """
        Initialize the REST connector.
        
        Args:
            config: CLOB configuration. If None, loads from environment.
        """
        self.config = config or get_config().clob
        
        super().__init__(
            name="polymarket_clob_rest",
            reconnect_base_delay=self.config.reconnect_delay_base_sec,
            reconnect_max_delay=self.config.reconnect_delay_max_sec
        )
        
        self._session: Optional[requests.Session] = None
        # Track multiple markets
        self._current_markets: Dict[str, MarketSpec] = {}
        self._token_to_market: Dict[str, str] = {}
        self._last_prices: Dict[str, float] = {}
        self._poll_count: int = 0
    
    @property
    def current_market(self) -> Optional[MarketSpec]:
        """Get the first configured market (for backwards compatibility)."""
        if not self._current_markets:
            return None
        return next(iter(self._current_markets.values()))
    
    @property
    def current_markets(self) -> Dict[str, MarketSpec]:
        """Get all currently configured markets."""
        return self._current_markets.copy()
    
    def add_market(self, market: MarketSpec) -> None:
        """
        Add a market to poll prices for.
        
        Args:
            market: Market specification with token IDs
        """
        if market.market_id in self._current_markets:
            return
        
        self._current_markets[market.market_id] = market
        
        # Track token to market mapping
        for token in market.get_token_ids():
            self._token_to_market[token] = market.market_id
        
        self.logger.info(
            f"Market added: {market.market_id[:16]}...",
            total_markets=len(self._current_markets)
        )
    
    def remove_market(self, market_id: str) -> None:
        """
        Remove a market from polling.
        
        Args:
            market_id: Market ID to remove
        """
        if market_id not in self._current_markets:
            return
        
        market = self._current_markets.pop(market_id)
        
        # Clean up token mapping
        for token in market.get_token_ids():
            if token in self._token_to_market:
                del self._token_to_market[token]
        
        self.logger.info(f"Market removed: {market_id[:16]}...")
    
    def set_market(self, market: MarketSpec) -> None:
        """
        Set the market to poll (backwards compatibility).
        
        Args:
            market: Market specification with token IDs
        """
        self.add_market(market)
    
    def set_tokens(self, market_id: str, token_ids: List[str]) -> None:
        """
        Set tokens to poll without a full MarketSpec.
        
        Args:
            market_id: Market condition ID
            token_ids: Token IDs to poll
        """
        for token in token_ids:
            self._token_to_market[token] = market_id
        
        self.logger.info(f"Tokens set for market {market_id[:16]}...", token_ids=token_ids)
    
    def _get_all_token_ids(self) -> List[str]:
        """Get all token IDs from all markets."""
        all_tokens = set()
        for market in self._current_markets.values():
            all_tokens.update(market.get_token_ids())
        return list(all_tokens)
    
    def _connect(self) -> None:
        """
        Main polling loop.
        
        Polls prices at configured interval until stopped.
        """
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "PolymarketDataConnector/1.0"
        })
        
        try:
            self._set_connected(True)
            self.logger.connected(self.config.rest_base_url)
            
            while not self._should_stop():
                token_ids = self._get_all_token_ids()
                if token_ids:
                    try:
                        self._poll_prices(token_ids)
                        self._update_heartbeat()
                    except requests.RequestException as e:
                        self.logger.error(f"Request error: {e}")
                    except Exception as e:
                        self.logger.exception(f"Poll error: {e}")
                
                # Wait for next poll
                if not self._wait(self.config.rest_poll_interval_sec):
                    break
                    
        finally:
            if self._session:
                self._session.close()
                self._session = None
    
    def _poll_prices(self, token_ids: List[str]) -> None:
        """
        Fetch current prices for specified tokens.
        
        Args:
            token_ids: List of token IDs to poll
        """
        for token_id in token_ids:
            try:
                price_data = self._fetch_token_price(token_id)
                
                if price_data:
                    self._process_price_data(token_id, price_data)
                    
            except Exception as e:
                self.logger.debug(f"Error fetching price for {token_id}: {e}")
        
        self._poll_count += 1
        self._update_last_message()
    
    def _fetch_token_price(self, token_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch price data for a single token.
        
        Uses the CLOB orderbook or price endpoint.
        
        Args:
            token_id: Token ID to fetch
            
        Returns:
            Price data dictionary or None
        """
        # Try the book endpoint first (provides bid/ask)
        try:
            url = f"{self.config.rest_base_url}/book"
            params = {"token_id": token_id}
            
            response = self._session.get(
                url,
                params=params,
                timeout=self.config.request_timeout_sec
            )
            
            if response.status_code == 200:
                return response.json()
                
        except Exception:
            pass
        
        # Fallback to prices endpoint
        try:
            url = f"{self.config.rest_base_url}/prices"
            params = {"token_ids": token_id}
            
            response = self._session.get(
                url,
                params=params,
                timeout=self.config.request_timeout_sec
            )
            
            if response.status_code == 200:
                data = response.json()
                if token_id in data:
                    return {"price": data[token_id]}
                    
        except Exception:
            pass
        
        return None
    
    def _process_price_data(self, token_id: str, data: Dict[str, Any]) -> None:
        """
        Process fetched price data and emit tick.
        
        Args:
            token_id: Token ID
            data: Price data from API
        """
        price = None
        
        # Try different price fields
        if "price" in data:
            price = float(data["price"])
        elif "bids" in data and "asks" in data:
            # Calculate mid from order book
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            if bids and asks:
                best_bid = float(bids[0]["price"]) if isinstance(bids[0], dict) else float(bids[0][0])
                best_ask = float(asks[0]["price"]) if isinstance(asks[0], dict) else float(asks[0][0])
                price = (best_bid + best_ask) / 2
        elif "market" in data:
            market_data = data["market"]
            price = float(market_data.get("price", 0))
        
        if price is None or price <= 0:
            return
        
        # Get market ID
        market_id = ""
        if self._current_market:
            market_id = self._current_market.market_id
        
        # Create and publish tick
        tick = MarketPriceTick(
            ts_ms=current_ts_ms(),
            market_id=market_id,
            token_id=token_id,
            price=price,
            side=None,
            source=SourceType.POLYMARKET_REST
        )
        
        self._last_prices[token_id] = price
        
        publish(TOPIC_POLYMARKET_PRICES, tick)
        self.logger.tick(tick.to_dict(), token_id=token_id, price=price)
    
    def get_last_price(self, token_id: str) -> Optional[float]:
        """
        Get last known price for a token.
        
        Args:
            token_id: Token ID
            
        Returns:
            Last price or None
        """
        return self._last_prices.get(token_id)
    
    def fetch_once(self, token_ids: Optional[List[str]] = None) -> Dict[str, float]:
        """
        Perform a single price fetch (useful for one-off queries).
        
        Args:
            token_ids: Tokens to fetch (uses configured tokens if None)
            
        Returns:
            Dictionary of token_id -> price
        """
        tokens = token_ids or self._token_ids
        
        if not tokens:
            return {}
        
        # Create temporary session if needed
        session = self._session or requests.Session()
        close_session = self._session is None
        
        try:
            prices = {}
            
            for token_id in tokens:
                try:
                    url = f"{self.config.rest_base_url}/prices"
                    params = {"token_ids": token_id}
                    
                    response = session.get(
                        url,
                        params=params,
                        timeout=self.config.request_timeout_sec
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if token_id in data:
                            prices[token_id] = float(data[token_id])
                            
                except Exception:
                    continue
            
            return prices
            
        finally:
            if close_session:
                session.close()


class PolymarketPriceFetcher:
    """
    Standalone price fetcher utility.
    
    Useful for one-off price queries without running a connector.
    """
    
    def __init__(self, config: Optional[PolymarketClobConfig] = None):
        """
        Initialize the price fetcher.
        
        Args:
            config: CLOB configuration
        """
        self.config = config or get_config().clob
    
    def fetch_prices(self, token_ids: List[str]) -> Dict[str, Optional[float]]:
        """
        Fetch prices for multiple tokens.
        
        Args:
            token_ids: List of token IDs
            
        Returns:
            Dictionary mapping token_id to price (or None if failed)
        """
        results = {token_id: None for token_id in token_ids}
        
        with requests.Session() as session:
            session.headers.update({
                "Accept": "application/json",
                "User-Agent": "PolymarketDataConnector/1.0"
            })
            
            for token_id in token_ids:
                try:
                    url = f"{self.config.rest_base_url}/prices"
                    params = {"token_ids": token_id}
                    
                    response = session.get(
                        url,
                        params=params,
                        timeout=self.config.request_timeout_sec
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if token_id in data:
                            results[token_id] = float(data[token_id])
                            
                except Exception:
                    continue
        
        return results
    
    def fetch_book(self, token_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch order book for a token.
        
        Args:
            token_id: Token ID
            
        Returns:
            Order book data or None
        """
        try:
            with requests.Session() as session:
                url = f"{self.config.rest_base_url}/book"
                params = {"token_id": token_id}
                
                response = session.get(
                    url,
                    params=params,
                    timeout=self.config.request_timeout_sec
                )
                
                if response.status_code == 200:
                    return response.json()
                    
        except Exception:
            pass
        
        return None
