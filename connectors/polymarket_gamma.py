"""
Polymarket Gamma API connector for market discovery.

Uses series-based queries to discover up/down crypto markets.
Each series (e.g., "btc-up-or-down-15m") contains recurring events
that are created every 15 minutes.
"""

import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set

import requests

from config import PolymarketGammaConfig, get_config
from models import MarketSpec, MarketStatus, current_ts_ms
from pubsub import TOPIC_MARKET_SPEC, TOPIC_MARKET_EXPIRED, publish
from connectors.base import BaseConnector


class PolymarketGammaConnector(BaseConnector):
    """
    REST polling connector for Polymarket Gamma API.
    
    Discovers active events using series IDs for efficient querying.
    Each series maps to a specific coin + duration (e.g., BTC 15M).
    
    Features:
    - Series-based discovery: Query events by series_id directly
    - Multi-series tracking: Track BTC 15M, ETH 15M, etc.
    - Auto-rotation: Publishes new events as they become active
    """
    
    def __init__(self, config: Optional[PolymarketGammaConfig] = None):
        """
        Initialize the Gamma connector.
        
        Args:
            config: Gamma API configuration. If None, loads from environment.
        """
        self.config = config or get_config().gamma
        
        super().__init__(
            name="polymarket_gamma",
            reconnect_base_delay=self.config.reconnect_delay_base_sec,
            reconnect_max_delay=self.config.reconnect_delay_max_sec
        )
        
        self._session: Optional[requests.Session] = None
        # Track markets by event_id (not market_id which is conditionId)
        self._current_events: Dict[str, MarketSpec] = {}
        # Track which series we've fetched
        self._series_events: Dict[str, List[str]] = {}  # series_key -> list of event_ids
        self._last_fetch_ts_ms: int = 0
        self._fetch_count: int = 0
    
    @property
    def current_market(self) -> Optional[MarketSpec]:
        """Get the first tracked market (for backwards compatibility)."""
        if not self._current_events:
            return None
        return next(iter(self._current_events.values()))
    
    @property
    def current_markets(self) -> Dict[str, MarketSpec]:
        """Get all currently tracked markets."""
        return self._current_events.copy()
    
    def get_market(self, market_id: str) -> Optional[MarketSpec]:
        """Get a specific market by market_id (conditionId)."""
        for event in self._current_events.values():
            if event.market_id == market_id:
                return event
        return None
    
    @property
    def last_fetch_time(self) -> int:
        """Get timestamp of last successful fetch in milliseconds."""
        return self._last_fetch_ts_ms
    
    def _connect(self) -> None:
        """
        Main polling loop for market discovery.
        
        Periodically queries series to find active events.
        """
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "PolymarketDataConnector/1.0"
        })
        
        # Disable SSL verification if configured
        if not self.config.ssl_verify:
            self._session.verify = False
            self.logger.warning("SSL verification disabled for REST requests")
            # Suppress InsecureRequestWarning
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        try:
            self._set_connected(True)
            self.logger.connected(self.config.api_base_url)
            
            # Log configured series
            series_ids = self.config.get_series_ids()
            self.logger.info(
                f"Configured series: {list(series_ids.keys())}",
                series_ids=series_ids
            )
            
            while not self._should_stop():
                try:
                    self._poll_all_series()
                    self._update_heartbeat()
                    
                except requests.RequestException as e:
                    self.logger.error(f"Request error: {e}")
                    self._set_error(str(e))
                except Exception as e:
                    self.logger.exception(f"Unexpected error in poll loop: {e}")
                
                # Wait for next poll interval
                if not self._wait(self.config.discovery_interval_sec):
                    break
                    
        finally:
            if self._session:
                self._session.close()
                self._session = None
    
    def _poll_all_series(self) -> None:
        """Poll all configured series for active events."""
        series_ids = self.config.get_series_ids()
        
        if not series_ids:
            self.logger.warning("No series IDs configured")
            return
        
        for series_key, series_id in series_ids.items():
            try:
                events = self._fetch_series_events(series_id, series_key)
                if events:
                    self._process_events(events, series_key)
            except Exception as e:
                self.logger.error(f"Error fetching series {series_key}: {e}")
        
        self._last_fetch_ts_ms = current_ts_ms()
        self._fetch_count += 1
        
        # Clean up expired events
        self._cleanup_expired_events()
    
    def _fetch_series_events(
        self, series_id: int, series_key: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch active events for a series.
        
        Args:
            series_id: Polymarket series ID
            series_key: Human-readable key like "BTC-15M"
            
        Returns:
            List of event data from API
        """
        url = f"{self.config.api_base_url}/events"
        params = {
            "series_id": series_id,
            "active": "true",
            "closed": "false",
            "limit": self.config.max_events_per_series,
            "order": "endDate",
            "ascending": "true",  # Nearest expiry first
        }
        
        self.logger.info(f"Fetching events for {series_key} (series_id={series_id})")
        
        response = self._session.get(
            url, params=params, timeout=self.config.request_timeout_sec
        )
        response.raise_for_status()
        
        data = response.json()
        self.logger.info(f"Fetched {len(data) if isinstance(data, list) else 0} events for {series_key}")
        
        # Handle response format
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "data" in data:
            return data["data"]
        return []
    
    def _process_events(
        self, events: List[Dict[str, Any]], series_key: str
    ) -> None:
        """
        Process events from a series and publish new markets.
        
        Args:
            events: List of event data from API
            series_key: Series identifier (e.g., "BTC-15M")
        """
        current_time_ms = current_ts_ms()
        processed_event_ids = []
        
        self.logger.info(f"Processing {len(events)} events for {series_key}")
        
        for event_data in events:
            event_id = event_data.get("id")
            if not event_id:
                continue
            
            processed_event_ids.append(event_id)
            
            # Skip if we already have this event
            if event_id in self._current_events:
                self.logger.debug(f"Event {event_id} already tracked")
                continue
            
            # Extract market data from event
            market_spec = self._parse_event_to_market(event_data, series_key)
            
            if market_spec is None:
                self.logger.warning(f"Could not parse event {event_id}")
                continue
            
            # Skip expired markets
            if market_spec.expiry_ts_ms <= current_time_ms:
                self.logger.debug(f"Event {event_id} is expired")
                continue
            
            # New event - track and publish
            self._current_events[event_id] = market_spec
            
            minutes_remaining = (market_spec.expiry_ts_ms - current_time_ms) / 60000
            self.logger.info(
                f"[NEW] {series_key} event: {market_spec.title[:50]}",
                event_id=event_id,
                market_id=market_spec.market_id[:16] + "...",
                minutes_remaining=round(minutes_remaining, 1),
                token_yes=market_spec.token_yes[:16] + "..." if market_spec.token_yes else None,
                token_no=market_spec.token_no[:16] + "..." if market_spec.token_no else None,
            )
            
            # Publish for CLOB WS to subscribe
            publish(TOPIC_MARKET_SPEC, market_spec)
        
        # Update series tracking
        self._series_events[series_key] = processed_event_ids
    
    def _parse_event_to_market(
        self, event_data: Dict[str, Any], series_key: str
    ) -> Optional[MarketSpec]:
        """
        Parse event data into a MarketSpec.
        
        Args:
            event_data: Event data from Gamma API
            series_key: Series identifier
            
        Returns:
            MarketSpec or None if parsing fails
        """
        try:
            markets = event_data.get("markets", [])
            if not markets:
                self.logger.warning(f"Event {event_data.get('id')} has no markets")
                return None
            
            # Take first market (up/down events have one market)
            market_data = markets[0]
            
            # Parse clobTokenIds (stored as JSON string in API)
            clob_token_ids = market_data.get("clobTokenIds")
            self.logger.debug(f"Raw clobTokenIds: {clob_token_ids}")
            
            if isinstance(clob_token_ids, str):
                try:
                    clob_token_ids = json.loads(clob_token_ids)
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to parse clobTokenIds: {e}")
                    clob_token_ids = []
            
            if not clob_token_ids or len(clob_token_ids) < 2:
                self.logger.warning(
                    f"Event {event_data.get('id')} has insufficient tokens: {clob_token_ids}"
                )
                return None
            
            # Parse expiry
            end_date_str = event_data.get("endDate")
            if end_date_str:
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                expiry_ts_ms = int(end_dt.timestamp() * 1000)
            else:
                expiry_ts_ms = 0
            
            # Build MarketSpec
            return MarketSpec(
                market_id=market_data.get("conditionId", ""),
                event_id=str(event_data.get("id", "")),
                title=event_data.get("title", "Unknown"),
                slug=event_data.get("slug", ""),
                strike=0.0,  # Up/down markets don't have a strike
                expiry_ts_ms=expiry_ts_ms,
                status=MarketStatus.ACTIVE if market_data.get("active") else MarketStatus.CLOSED,
                token_yes=clob_token_ids[0] if len(clob_token_ids) > 0 else None,
                token_no=clob_token_ids[1] if len(clob_token_ids) > 1 else None,
                extra={
                    "series_key": series_key,
                    "event_id": event_data.get("id"),
                    "event_slug": event_data.get("slug"),
                    "condition_id": market_data.get("conditionId"),
                }
            )
            
        except Exception as e:
            self.logger.warning(f"Error parsing event {event_data.get('id')}: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return None
    
    def _cleanup_expired_events(self) -> None:
        """Remove expired events from tracking and notify subscribers."""
        current_time_ms = current_ts_ms()
        expired_markets = []
        
        for event_id, market in list(self._current_events.items()):
            if market.expiry_ts_ms <= current_time_ms:
                expired_markets.append(market)
                del self._current_events[event_id]
                self.logger.info(f"Event expired and removed: {event_id}")
        
        # Publish expired events so CLOB WS can unsubscribe
        for market in expired_markets:
            publish(TOPIC_MARKET_EXPIRED, market)
            self.logger.info(
                f"Published expired market: {market.market_id[:16]}...",
                event_id=market.event_id,
                market_id=market.market_id
            )
        
        if expired_markets:
            self.logger.debug(f"Cleaned up {len(expired_markets)} expired events")
    
    def force_refresh(self) -> Optional[MarketSpec]:
        """
        Force an immediate refresh.
        
        Returns:
            First current market after refresh
        """
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "Accept": "application/json",
                "User-Agent": "PolymarketDataConnector/1.0"
            })
        
        try:
            self._poll_all_series()
            return self.current_market
        except Exception as e:
            self.logger.error(f"Force refresh failed: {e}")
            return None


class MarketSpecDeduplicator:
    """
    Helper class for deduplicating market spec updates.
    
    Useful for downstream consumers that want to track market changes
    without duplicates.
    """
    
    def __init__(self):
        self._seen_market_ids: Set[str] = set()
        self._change_count: int = 0
    
    def is_new(self, market: MarketSpec) -> bool:
        """
        Check if this is a new (not seen) market.
        
        Args:
            market: Market spec to check
            
        Returns:
            True if this market hasn't been seen before
        """
        if market.market_id not in self._seen_market_ids:
            self._seen_market_ids.add(market.market_id)
            self._change_count += 1
            return True
        return False
    
    @property
    def change_count(self) -> int:
        """Get the number of new markets observed."""
        return self._change_count
    
    def reset(self) -> None:
        """Reset the deduplicator state."""
        self._seen_market_ids.clear()
        self._change_count = 0
