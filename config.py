"""
Configuration management for the market data connectors.

Loads configuration from environment variables or .env file.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Optional
from pathlib import Path


def _load_env_file(env_path: str = ".env") -> None:
    """Load environment variables from .env file if it exists."""
    path = Path(env_path)
    if not path.exists():
        return
    
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value


def _get_env(key: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.environ.get(key, default)


def _get_env_int(key: str, default: int) -> int:
    """Get environment variable as integer with default."""
    try:
        return int(os.environ.get(key, str(default)))
    except (ValueError, TypeError):
        return default


def _get_env_float(key: str, default: float) -> float:
    """Get environment variable as float with default."""
    try:
        return float(os.environ.get(key, str(default)))
    except (ValueError, TypeError):
        return default


def _get_env_bool(key: str, default: bool) -> bool:
    """Get environment variable as boolean with default."""
    val = os.environ.get(key, str(default)).lower()
    return val in ("true", "1", "yes", "on")


@dataclass(frozen=True)
class BinanceConfig:
    """Configuration for Binance WebSocket connector."""
    
    # WebSocket endpoint
    ws_base_url: str = "wss://stream.binance.com:9443"
    
    # Symbols to subscribe to (comma-separated list)
    symbols: tuple = ("btcusdt", "ethusdt")
    
    # Connection settings
    ping_interval_sec: float = 30.0
    ping_timeout_sec: float = 10.0
    reconnect_delay_base_sec: float = 1.0
    reconnect_delay_max_sec: float = 60.0
    
    # SSL verification (set to False if behind corporate proxy)
    ssl_verify: bool = True
    
    @classmethod
    def from_env(cls) -> "BinanceConfig":
        """Create configuration from environment variables."""
        symbols_str = _get_env("BINANCE_SYMBOLS", "btcusdt,ethusdt")
        symbols = tuple(s.strip().lower() for s in symbols_str.split(",") if s.strip())
        
        return cls(
            ws_base_url=_get_env("BINANCE_WS_URL", "wss://stream.binance.com:9443"),
            symbols=symbols,
            ping_interval_sec=_get_env_float("BINANCE_PING_INTERVAL_SEC", 30.0),
            ping_timeout_sec=_get_env_float("BINANCE_PING_TIMEOUT_SEC", 10.0),
            reconnect_delay_base_sec=_get_env_float("BINANCE_RECONNECT_DELAY_BASE_SEC", 1.0),
            reconnect_delay_max_sec=_get_env_float("BINANCE_RECONNECT_DELAY_MAX_SEC", 60.0),
            ssl_verify=_get_env_bool("SSL_VERIFY", True),
        )

    @property
    def stream_url(self) -> str:
        """Get the combined WebSocket stream URL for all symbols."""
        streams = "/".join(f"{s}@bookTicker" for s in self.symbols)
        return f"{self.ws_base_url}/stream?streams={streams}"
    
    def get_single_stream_url(self, symbol: str) -> str:
        """Get WebSocket URL for a single symbol."""
        return f"{self.ws_base_url}/ws/{symbol.lower()}@bookTicker"

    USDT_USD_PROXY = "usdcusdt"

    @staticmethod
    def get_binance_symbols_for_series(target_series: tuple, include_usdt_usd: bool = True) -> tuple:
        """Get Binance symbol format for configured Polymarket series.
        
        Maps series like ("BTC", "15M") to Binance format "btcusdt".
        Also includes USDTUSD for USDT/USD conversion if include_usdt_usd=True.
        
        Args:
            target_series: Tuple of (coin, duration) pairs from PolymarketGammaConfig
            include_usdt_usd: Whether to include USDTUSD for USD price conversion
            
        Returns:
            Tuple of Binance symbol strings (e.g., ("btcusdt", "ethusdt", "usdtusd"))
        """
        symbols = set()
        for coin, duration in target_series:
            symbols.add(f"{coin.lower()}usdt")
        
        # Always include USDT/USD proxy for USD conversion
        if include_usdt_usd:
            symbols.add(BinanceConfig.USDT_USD_PROXY)
        
        return tuple(sorted(symbols))

    def with_symbols_from_series(self, target_series: tuple) -> "BinanceConfig":
        """Return a new BinanceConfig with symbols derived from Polymarket series.
        
        Args:
            target_series: Tuple of (coin, duration) pairs from PolymarketGammaConfig
            
        Returns:
            New BinanceConfig instance with derived symbols
        """
        derived_symbols = self.get_binance_symbols_for_series(target_series)
        return BinanceConfig(
            ws_base_url=self.ws_base_url,
            symbols=derived_symbols,
            ping_interval_sec=self.ping_interval_sec,
            ping_timeout_sec=self.ping_timeout_sec,
            reconnect_delay_base_sec=self.reconnect_delay_base_sec,
            reconnect_delay_max_sec=self.reconnect_delay_max_sec,
            ssl_verify=self.ssl_verify,
        )


@dataclass(frozen=True)
class PolymarketGammaConfig:
    """Configuration for Polymarket Gamma API polling.
    
    Uses series-based discovery for up/down crypto markets.
    Series IDs map directly to specific coin + duration combinations:
    - BTC 15M: series_id=10192 (slug: btc-up-or-down-15m)
    - ETH 15M: series_id=10191 (slug: eth-up-or-down-15m)
    - BTC 5M:  series_id=10684 (slug: btc-up-or-down-5m)
    - BTC Daily: series_id=41 (slug: btc-up-or-down-daily)
    """
    
    # API endpoint
    api_base_url: str = "https://gamma-api.polymarket.com"
    
    # Polling interval for checking new events
    discovery_interval_sec: float = 30.0
    
    # Series to subscribe to - tuple of (coin, duration) e.g., [("BTC", "15M"), ("ETH", "15M")]
    # Duration can be: "5M", "15M", "DAILY"
    target_series: tuple = (("ETH", "15M"),)
    
    # Maximum number of active events to track per series
    max_events_per_series: int = 3
    
    # Request settings
    request_timeout_sec: float = 10.0
    reconnect_delay_base_sec: float = 1.0
    reconnect_delay_max_sec: float = 300.0
    
    # SSL verification (set to False if behind corporate proxy)
    ssl_verify: bool = True
    
    @classmethod
    def from_env(cls) -> "PolymarketGammaConfig":
        """Create configuration from environment variables.
        
        Env vars:
            POLYMARKET_SERIES: Comma-separated list like "BTC-15M,ETH-15M"
        """
        # Parse series config from env, or use class default
        series_str = _get_env("POLYMARKET_SERIES", "")
        
        if series_str:
            # Parse from env var (e.g., "BTC-15M,ETH-15M")
            target_series = []
            for s in series_str.split(","):
                s = s.strip().upper()
                if "-" in s:
                    coin, duration = s.split("-", 1)
                    target_series.append((coin.strip(), duration.strip()))
            target_series = tuple(target_series) if target_series else cls.target_series
        else:
            # Use class default
            target_series = cls.target_series
        
        return cls(
            api_base_url=_get_env("POLYMARKET_GAMMA_API_URL", "https://gamma-api.polymarket.com"),
            discovery_interval_sec=_get_env_float("POLYMARKET_DISCOVERY_INTERVAL_SEC", 30.0),
            target_series=tuple(target_series),
            max_events_per_series=_get_env_int("POLYMARKET_MAX_EVENTS_PER_SERIES", 3),
            request_timeout_sec=_get_env_float("POLYMARKET_REQUEST_TIMEOUT_SEC", 10.0),
            reconnect_delay_base_sec=_get_env_float("POLYMARKET_RECONNECT_DELAY_BASE_SEC", 1.0),
            reconnect_delay_max_sec=_get_env_float("POLYMARKET_RECONNECT_DELAY_MAX_SEC", 300.0),
            ssl_verify=_get_env_bool("SSL_VERIFY", True),
        )
    
    def get_series_ids(self) -> Dict[str, int]:
        """Get Polymarket series IDs for configured targets.
        
        Returns:
            Dict mapping series key (e.g., "BTC-15M") to series_id
        """
        # Known series IDs (discovered from Polymarket API)
        SERIES_MAP = {
            ("BTC", "15M"): 10192,
            ("ETH", "15M"): 10191,
            ("BTC", "DAILY"): 41,
            ("ETH", "DAILY"): 42,  # Assumed pattern
        }
        
        result = {}
        for coin, duration in self.target_series:
            key = (coin.upper(), duration.upper())
            if key in SERIES_MAP:
                result[f"{coin}-{duration}"] = SERIES_MAP[key]
        
        return result


@dataclass(frozen=True)
class RTDSConfig:
    """Configuration for Polymarket RTDS (Real-Time Data Service) connector.
    
    Provides access to Chainlink oracle price feeds via WebSocket.
    Symbols are automatically derived from the target series in PolymarketGammaConfig.
    Docs: https://docs.polymarket.com/developers/RTDS/RTDS-crypto-prices
    """
    
    # WebSocket endpoint for RTDS
    ws_base_url: str = "wss://ws-live-data.polymarket.com"
    
    # Connection settings
    ping_interval_sec: float = 30.0
    reconnect_delay_base_sec: float = 1.0
    reconnect_delay_max_sec: float = 60.0
    
    # SSL verification
    ssl_verify: bool = True
    
    @classmethod
    def from_env(cls) -> "RTDSConfig":
        """Create configuration from environment variables."""
        return cls(
            ws_base_url=_get_env("RTDS_WS_URL", "wss://ws-live-data.polymarket.com"),
            ping_interval_sec=_get_env_float("RTDS_PING_INTERVAL_SEC", 30.0),
            reconnect_delay_base_sec=_get_env_float("RTDS_RECONNECT_DELAY_BASE_SEC", 1.0),
            reconnect_delay_max_sec=_get_env_float("RTDS_RECONNECT_DELAY_MAX_SEC", 60.0),
            ssl_verify=_get_env_bool("SSL_VERIFY", True),
        )
    
    @staticmethod
    def get_chainlink_symbols_for_series(target_series: tuple) -> tuple:
        """Get Chainlink symbol format for configured series.
        
        Maps series like ("BTC", "15M") to Chainlink format "btc/usd".
        
        Args:
            target_series: Tuple of (coin, duration) pairs from PolymarketGammaConfig
            
        Returns:
            Tuple of Chainlink symbol strings (e.g., ("btc/usd", "eth/usd"))
        """
        coins = set()
        for coin, duration in target_series:
            coins.add(coin.lower())
        return tuple(f"{coin}/usd" for coin in sorted(coins))


@dataclass(frozen=True)
class PolymarketClobConfig:
    """Configuration for Polymarket CLOB WebSocket/REST connectors."""
    
    # WebSocket endpoint
    ws_base_url: str = "wss://ws-subscriptions-clob.polymarket.com"
    
    # REST endpoint
    rest_base_url: str = "https://clob.polymarket.com"
    
    # API credentials (optional, for authenticated channels)
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    api_passphrase: Optional[str] = None
    
    # Connection settings
    ping_interval_sec: float = 10.0
    reconnect_delay_base_sec: float = 1.0
    reconnect_delay_max_sec: float = 60.0
    
    # REST polling fallback
    rest_poll_interval_sec: float = 5.0
    request_timeout_sec: float = 10.0
    
    # Health check settings
    ws_unhealthy_threshold_sec: float = 30.0
    
    # SSL verification (set to False if behind corporate proxy)
    ssl_verify: bool = True
    
    @classmethod
    def from_env(cls) -> "PolymarketClobConfig":
        """Create configuration from environment variables."""
        return cls(
            ws_base_url=_get_env("POLYMARKET_CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com"),
            rest_base_url=_get_env("POLYMARKET_CLOB_REST_URL", "https://clob.polymarket.com"),
            api_key=_get_env("POLYMARKET_API_KEY", "") or None,
            api_secret=_get_env("POLYMARKET_API_SECRET", "") or None,
            api_passphrase=_get_env("POLYMARKET_API_PASSPHRASE", "") or None,
            ping_interval_sec=_get_env_float("POLYMARKET_PING_INTERVAL_SEC", 10.0),
            reconnect_delay_base_sec=_get_env_float("POLYMARKET_RECONNECT_DELAY_BASE_SEC", 1.0),
            reconnect_delay_max_sec=_get_env_float("POLYMARKET_RECONNECT_DELAY_MAX_SEC", 60.0),
            rest_poll_interval_sec=_get_env_float("POLYMARKET_REST_POLL_INTERVAL_SEC", 5.0),
            request_timeout_sec=_get_env_float("POLYMARKET_REQUEST_TIMEOUT_SEC", 10.0),
            ws_unhealthy_threshold_sec=_get_env_float("POLYMARKET_WS_UNHEALTHY_THRESHOLD_SEC", 30.0),
            ssl_verify=_get_env_bool("SSL_VERIFY", True),
        )


@dataclass(frozen=True)
class LoggingConfig:
    """Configuration for logging."""
    
    # Log file path
    log_path: str = "./logs"
    
    # Log file name
    log_file_name: str = "connectors.jsonl"
    
    # Log rotation settings
    max_file_size_mb: int = 100
    backup_count: int = 5
    
    # Console logging
    console_enabled: bool = True
    console_level: str = "INFO"
    
    # File logging level
    file_level: str = "DEBUG"
    
    @classmethod
    def from_env(cls) -> "LoggingConfig":
        """Create configuration from environment variables."""
        return cls(
            log_path=_get_env("LOG_PATH", "./logs"),
            log_file_name=_get_env("LOG_FILE_NAME", "connectors.jsonl"),
            max_file_size_mb=_get_env_int("LOG_MAX_FILE_SIZE_MB", 100),
            backup_count=_get_env_int("LOG_BACKUP_COUNT", 5),
            console_enabled=_get_env_bool("LOG_CONSOLE_ENABLED", True),
            console_level=_get_env("LOG_CONSOLE_LEVEL", "INFO"),
            file_level=_get_env("LOG_FILE_LEVEL", "DEBUG"),
        )


@dataclass(frozen=True)
class AppConfig:
    """Main application configuration.
    
    Important: Binance and RTDS symbols are derived from the Polymarket target series.
    This ensures we only subscribe to feeds for coins we're actually trading on Polymarket.
    
    Example: If gamma.target_series = [("BTC", "15M"), ("ETH", "1H")]
        - Binance will subscribe to: btcusdt, ethusdt
        - Chainlink will subscribe to: btc/usd, eth/usd
    """
    
    binance: BinanceConfig = field(default_factory=BinanceConfig.from_env)
    gamma: PolymarketGammaConfig = field(default_factory=PolymarketGammaConfig.from_env)
    clob: PolymarketClobConfig = field(default_factory=PolymarketClobConfig.from_env)
    rtds: RTDSConfig = field(default_factory=RTDSConfig.from_env)
    logging: LoggingConfig = field(default_factory=LoggingConfig.from_env)
    
    # Status reporting interval
    status_interval_sec: float = 10.0
    
    # Health monitoring
    health_check_interval_sec: float = 5.0
    
    def get_subscribed_coins(self) -> tuple:
        """Get the unique coins from target series.
        
        Returns:
            Tuple of coin symbols (e.g., ("BTC", "ETH"))
        """
        coins = set()
        for coin, duration in self.gamma.target_series:
            coins.add(coin.upper())
        return tuple(sorted(coins))
    
    def get_binance_symbols(self) -> tuple:
        """Get Binance symbols derived from Polymarket target series."""
        return BinanceConfig.get_binance_symbols_for_series(self.gamma.target_series)
    
    def get_chainlink_symbols(self) -> tuple:
        """Get Chainlink symbols derived from Polymarket target series."""
        return RTDSConfig.get_chainlink_symbols_for_series(self.gamma.target_series)
    
    @classmethod
    def from_env(cls, env_file: str = ".env") -> "AppConfig":
        """Create full configuration from environment variables.
        
        Binance and RTDS symbols are automatically derived from the Polymarket
        target series (POLYMARKET_SERIES env var or default).
        """
        _load_env_file(env_file)
        
        # Load Gamma config first to derive other configs from its target_series
        gamma_config = PolymarketGammaConfig.from_env()
        
        # Create Binance config with symbols derived from Polymarket series
        base_binance = BinanceConfig.from_env()
        binance_config = base_binance.with_symbols_from_series(gamma_config.target_series)
        
        return cls(
            binance=binance_config,
            gamma=gamma_config,
            clob=PolymarketClobConfig.from_env(),
            rtds=RTDSConfig.from_env(),
            logging=LoggingConfig.from_env(),
            status_interval_sec=_get_env_float("STATUS_INTERVAL_SEC", 10.0),
            health_check_interval_sec=_get_env_float("HEALTH_CHECK_INTERVAL_SEC", 5.0),
        )


# Global config instance (lazy loaded)
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = AppConfig.from_env()
    return _config


def reset_config() -> None:
    """Reset global config (useful for testing)."""
    global _config
    _config = None
