# Polymarket Data Connectors

A production-ready Python project for streaming **T-minute crypto binary options** from **Polymarket** alongside real-time spot prices from **Binance**. The ultimate goal is to **price these binary options** using the Binance data feed and generate signals when options are oversold/overbought.

## Project Goal

Track short-term crypto prediction markets on Polymarket (e.g., "Bitcoin Up or Down in 15 mins") and combine with real-time Binance price data. This enables:

1. **Binary Option Pricing**: Use spot price volatility to estimate fair value
2. **Signal Generation**: Detect when market prices deviate from fair value
3. **Arbitrage Detection**: Identify oversold/overbought conditions

## Features

- **T-Minute Market Discovery**: Automatically find and track short-term crypto markets (configurable duration)
- **Multi-Coin Support**: Configure target coins (BTC, ETH, SOL, etc.)
- **Multi-Symbol Binance Connector**: Stream best bid/ask for multiple trading pairs
- **Real-time Price Streaming**: WebSocket streaming with automatic REST fallback
- **Thread-safe Architecture**: Uses `threading.Thread`, `queue.Queue`, `threading.Event`, and locks
- **Pub/Sub Event Bus**: Loosely coupled components via queue-based publish/subscribe
- **Exponential Backoff**: Reconnection with jitter for all connectors
- **Structured Logging**: JSONL format with detailed filtering diagnostics
- **Health Monitoring**: Automatic fallback when WebSocket becomes unhealthy

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Usage Examples](#usage-examples)
- [Data Models](#data-models)
- [Architecture](#architecture)
- [Pub/Sub Topics](#pubsub-topics)
- [Running Tests](#running-tests)
- [Logging](#logging)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repo-url>
cd polymarket

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your settings (see Configuration section below)
```

### 3. Run the Application

```bash
python app.py
```

### 4. Example Output

```
[2024-02-06 12:30:00] Markets: 3 | Binance: BTC:$65,973(42ms) | ETH:$2,845(38ms) | PM: 6 tokens (150ms ago) | Health: BIN:✓ GAM:✓ WS:✓
```

## Project Structure

```
polymarket/
├── app.py                      # Main orchestrator - wires everything together
├── config.py                   # Configuration management (loads from .env)
├── models.py                   # Data models (immutable dataclasses)
├── pubsub.py                   # Thread-safe pub/sub event bus
├── logging_utils.py            # JSONL structured logging
├── requirements.txt            # Python dependencies
├── .env.example                # Example configuration file
├── connectors/
│   ├── __init__.py
│   ├── base.py                 # BaseConnector abstract class
│   ├── binance_ws.py           # Binance WebSocket (multi-symbol)
│   ├── polymarket_gamma.py     # Gamma API market discovery (multi-market)
│   ├── polymarket_clob_ws.py   # CLOB WebSocket connector (multi-market)
│   └── polymarket_clob_rest.py # CLOB REST fallback (multi-market)
├── tests/
│   ├── __init__.py
│   └── test_connectors.py      # Unit tests (31 tests)
└── logs/
    └── connectors.jsonl        # Structured logs (auto-created)
```

## Configuration

All configuration is done via environment variables. Copy `.env.example` to `.env` and customize:

### Binance Configuration

```bash
# WebSocket base URL (default: wss://stream.binance.com:9443)
BINANCE_WS_URL=wss://stream.binance.com:9443

# Symbols to subscribe to (comma-separated, lowercase)
# Examples: btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt
BINANCE_SYMBOLS=btcusdt,ethusdt

# Connection settings
BINANCE_PING_INTERVAL_SEC=30.0      # Keep-alive ping interval
BINANCE_RECONNECT_DELAY_BASE_SEC=1.0  # Initial reconnect delay
BINANCE_RECONNECT_DELAY_MAX_SEC=60.0  # Maximum reconnect delay
```

### Polymarket Configuration

```bash
# Gamma API for market discovery
POLYMARKET_GAMMA_API_URL=https://gamma-api.polymarket.com
POLYMARKET_DISCOVERY_INTERVAL_SEC=30.0  # How often to check for new markets

# =============================================================================
# TARGET MARKET CONFIGURATION (T-minute crypto binary options)
# =============================================================================

# Target coins to track (comma-separated tickers)
# These are matched against market questions/slugs
POLYMARKET_TARGET_COINS=BTC

# Target market duration in minutes (e.g., 15 for "15-minute" markets)
# Matches markets like "Bitcoin Up or Down in 15 mins"
POLYMARKET_TARGET_DURATION_MINUTES=15

# Duration tolerance in minutes (for fuzzy matching)
POLYMARKET_DURATION_TOLERANCE_MINUTES=5

# Strict duration matching (true/false)
# If true: only match markets with explicit duration patterns
# If false: also match markets expiring within target timeframe
POLYMARKET_STRICT_DURATION_MATCH=false

# Maximum number of markets to track simultaneously
POLYMARKET_MAX_MARKETS=10

# CLOB WebSocket and REST endpoints
POLYMARKET_CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com
POLYMARKET_CLOB_REST_URL=https://clob.polymarket.com

# Health monitoring - if WS unhealthy for this long, activate REST fallback
POLYMARKET_WS_UNHEALTHY_THRESHOLD_SEC=30.0
```

### Logging Configuration

```bash
LOG_PATH=./logs                    # Log directory
LOG_FILE_NAME=connectors.jsonl     # Log file name
LOG_MAX_FILE_SIZE_MB=100           # Max file size before rotation
LOG_BACKUP_COUNT=5                 # Number of backup files to keep
LOG_CONSOLE_ENABLED=true           # Enable console logging
LOG_CONSOLE_LEVEL=INFO             # Console log level
LOG_FILE_LEVEL=DEBUG               # File log level
```

## Usage Examples

### Basic Usage - Run the Orchestrator

```python
from app import ConnectorOrchestrator

# Create and run the orchestrator
orchestrator = ConnectorOrchestrator()
orchestrator.run_forever()  # Runs until Ctrl+C
```

### Custom Configuration

```python
from config import AppConfig, BinanceConfig, PolymarketGammaConfig

# Create custom config
config = AppConfig(
    binance=BinanceConfig(
        symbols=("btcusdt", "ethusdt", "solusdt"),
        ping_interval_sec=20.0
    ),
    gamma=PolymarketGammaConfig(
        search_keywords=("BTC", "Bitcoin", "ETH", "Ethereum"),
        max_markets=5
    )
)

orchestrator = ConnectorOrchestrator(config)
orchestrator.start()
```

### Subscribe to Price Events

```python
from pubsub import subscribe, TOPIC_BINANCE_TICKS, TOPIC_POLYMARKET_PRICES
import queue

# Subscribe to Binance ticks
binance_sub = subscribe(TOPIC_BINANCE_TICKS, "my_binance_subscriber")

# Subscribe to Polymarket prices
pm_sub = subscribe(TOPIC_POLYMARKET_PRICES, "my_pm_subscriber")

# Process events
while True:
    try:
        # Get Binance tick (non-blocking with timeout)
        tick = binance_sub.get(timeout=1.0)
        print(f"Binance {tick.symbol}: ${tick.mid:,.2f}")
    except queue.Empty:
        pass
    
    try:
        # Get Polymarket price
        pm_tick = pm_sub.get(timeout=1.0)
        print(f"Polymarket {pm_tick.token_id[:16]}: {pm_tick.price:.4f}")
    except queue.Empty:
        pass
```

### Subscribe to Market Discovery Events

```python
from pubsub import subscribe, TOPIC_MARKET_SPEC
from models import MarketSpec

# Get notified when new markets are discovered
market_sub = subscribe(TOPIC_MARKET_SPEC, "market_watcher")

while True:
    market: MarketSpec = market_sub.get()
    print(f"New market discovered: {market.title}")
    print(f"  ID: {market.market_id}")
    print(f"  Expiry: {market.expiry_ts_ms}")
    print(f"  Token YES: {market.token_yes}")
    print(f"  Token NO: {market.token_no}")
```

### Use Individual Connectors

```python
from connectors.binance_ws import BinanceWebSocketConnector
from config import BinanceConfig

# Create a standalone Binance connector
config = BinanceConfig(symbols=("btcusdt", "ethusdt"))
connector = BinanceWebSocketConnector(config)

# Start the connector
connector.start()

# Check health
print(f"Healthy: {connector.is_healthy()}")

# Get latest ticks
for symbol, tick in connector.last_ticks.items():
    print(f"{symbol}: ${tick.mid:,.2f}")

# Stop when done
connector.stop()
```

## Data Models

### PriceTick (Binance)

```python
from models import PriceTick, SourceType

tick = PriceTick(
    ts_ms=1707235200000,         # Timestamp in milliseconds
    symbol="BTCUSDT",            # Trading pair
    bid=65900.0,                 # Best bid price
    ask=65901.0,                 # Best ask price
    mid=65900.5,                 # Mid price (bid + ask) / 2
    source=SourceType.BINANCE   # Data source
)

# Serialize to dict (for logging/JSON)
print(tick.to_dict())
```

### MarketSpec (Polymarket Market)

```python
from models import MarketSpec, MarketStatus

market = MarketSpec(
    market_id="0xabc123...",              # Condition ID
    event_id="event_456",                  # Parent event
    slug="btc-above-70k-by-march",        # URL slug
    title="Will BTC be above $70,000?",   # Human-readable title
    expiry_ts_ms=1709251200000,           # Expiration timestamp
    strike=70000.0,                        # Strike price (if applicable)
    token_yes="12345...",                 # YES outcome token ID
    token_no="67890...",                  # NO outcome token ID
    status=MarketStatus.ACTIVE,           # Market status
    is_active=True,                       # Is market active
    accepts_orders=True                   # Can place orders
)

# Get token IDs for subscription
yes_id, no_id = market.get_token_ids()
```

### MarketPriceTick (Polymarket Price)

```python
from models import MarketPriceTick, SourceType

price = MarketPriceTick(
    ts_ms=1707235200000,
    market_id="0xabc123...",
    token_id="12345...",              # Token ID (YES or NO)
    price=0.65,                       # Price (0.0 to 1.0)
    side=None,                        # Side (if from orderbook)
    source=SourceType.POLYMARKET_WS   # WebSocket or REST
)
```

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    ConnectorOrchestrator                     │
│  - Manages lifecycle of all connectors                       │
│  - Health monitoring & REST fallback                        │
│  - Status display                                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┬─────────────────────────┐
        ▼             ▼             ▼                         ▼
┌───────────────┐ ┌───────────┐ ┌───────────────┐ ┌───────────────┐
│ BinanceWS     │ │ GammaAPI  │ │ CLOB WS       │ │ CLOB REST     │
│ Connector     │ │ Connector │ │ Connector     │ │ (fallback)    │
│               │ │           │ │               │ │               │
│ Multi-symbol  │ │ Discovery │ │ Multi-market  │ │ Multi-market  │
└───────┬───────┘ └─────┬─────┘ └───────┬───────┘ └───────┬───────┘
        │               │               │                 │
        ▼               ▼               ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     EventBus (Pub/Sub)                       │
│  Topics: binance_ticks, market_spec, polymarket_prices      │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Threading over Asyncio**: Simpler debugging, better library compatibility
2. **Pub/Sub Pattern**: Loose coupling between components
3. **Immutable Data Models**: Thread-safe by design using frozen dataclasses
4. **Graceful Degradation**: Automatic REST fallback when WebSocket fails
5. **Exponential Backoff**: With jitter to prevent thundering herd

### Fallback Mechanism

When the Polymarket WebSocket is unhealthy for more than `POLYMARKET_WS_UNHEALTHY_THRESHOLD_SEC` seconds:

1. The orchestrator detects the unhealthy state
2. REST polling connector is automatically activated
3. REST connector polls prices at configured interval
4. When WebSocket recovers, REST polling is deactivated
5. No manual intervention required

## Pub/Sub Topics

| Topic | Event Type | Description |
|-------|-----------|-------------|
| `binance_ticks` | `PriceTick` | Real-time Binance price updates |
| `market_spec` | `MarketSpec` | New market discoveries |
| `polymarket_prices` | `MarketPriceTick` | Polymarket price updates |
| `health` | `HealthEvent` | Connector health changes |

## Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage report
python -m pytest tests/ --cov=. --cov-report=term-missing

# Run specific test class
python -m pytest tests/test_connectors.py::TestPubSub -v

# Run specific test
python -m pytest tests/test_connectors.py::TestBackoffCalculator::test_exponential_growth -v
```

## Logging

Logs are written in JSONL format for easy parsing and analysis:

```json
{"timestamp":"2024-02-06T12:30:00.000Z","ts_ms":1707235200000,"level":"INFO","logger":"connector.binance_ws","message":"Connected","connector":"binance_ws","symbols":["btcusdt","ethusdt"]}
{"timestamp":"2024-02-06T12:30:00.100Z","ts_ms":1707235200100,"level":"INFO","logger":"connector.polymarket_gamma","message":"New market: Will BTC hit $70k?","market_id":"0xabc..."}
```

### Parsing Logs

```bash
# View all errors
cat logs/connectors.jsonl | jq 'select(.level == "ERROR")'

# View Binance ticks
cat logs/connectors.jsonl | jq 'select(.logger == "connector.binance_ws")'

# Count events by level
cat logs/connectors.jsonl | jq -r '.level' | sort | uniq -c
```

## Troubleshooting

### No Markets Found

If no Polymarket markets are discovered:
1. Check `POLYMARKET_SEARCH_KEYWORDS` includes relevant terms
2. Verify Polymarket has active markets matching your keywords
3. Check logs for "No matching markets found" messages

### WebSocket Disconnections

If WebSocket keeps disconnecting:
1. Check network connectivity
2. Verify API endpoints are correct
3. Look for rate limiting messages in logs
4. The system will automatically reconnect with exponential backoff

### High Latency

If data is delayed:
1. Check `*_ago` values in status output
2. Verify no REST fallback is active (indicates WS issues)
3. Check system resources (CPU, memory)

## API Reference

### ConnectorOrchestrator

```python
class ConnectorOrchestrator:
    def start(self) -> None: ...
    def stop(self, timeout: float = 10.0) -> None: ...
    def run_forever(self) -> None: ...
    def get_all_health(self) -> Dict[str, ConnectorHealth]: ...
```

### BaseConnector

```python
class BaseConnector(ABC):
    def start(self) -> None: ...
    def stop(self, timeout: float = 5.0) -> None: ...
    def is_healthy(self) -> bool: ...
    def get_health(self) -> ConnectorHealth: ...
    @property
    def last_heartbeat(self) -> int: ...
```

### EventBus

```python
def subscribe(topic: str, subscriber_name: str) -> Subscription: ...
def unsubscribe(subscription: Subscription) -> None: ...
def publish(topic: str, message: Any) -> int: ...
```

## Requirements

- Python 3.10+
- websocket-client
- requests
- python-dotenv (optional, for .env file loading)

## License

MIT
