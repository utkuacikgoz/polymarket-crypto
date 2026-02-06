"""
Unit tests for the market data connectors.

Tests cover:
- Pub/sub publish/subscribe functionality
- Market spec deduplication logic
- Connector backoff logic with deterministic seeding
- Data model serialization
"""

import json
import queue
import time
import unittest
from threading import Thread
from unittest.mock import MagicMock, patch

# Import modules under test
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    PriceTick, MarketSpec, MarketPriceTick, MarketSnapshot,
    ConnectorHealth, HealthEvent, SourceType, MarketStatus, Side,
    current_ts_ms
)
from pubsub import EventBus, Subscription, reset_event_bus, get_event_bus, publish, subscribe
from connectors.base import BackoffCalculator
from connectors.polymarket_gamma import MarketSpecDeduplicator


class TestPubSub(unittest.TestCase):
    """Tests for the pub/sub event bus."""
    
    def setUp(self):
        """Reset the event bus before each test."""
        reset_event_bus()
    
    def tearDown(self):
        """Clean up after each test."""
        reset_event_bus()
    
    def test_subscribe_and_publish(self):
        """Test basic subscribe and publish functionality."""
        bus = get_event_bus()
        
        # Subscribe to a topic
        sub = bus.subscribe("test_topic", "test_subscriber")
        
        self.assertIsInstance(sub, Subscription)
        self.assertEqual(sub.topic, "test_topic")
        self.assertEqual(sub.subscriber_id, "test_subscriber")
        
        # Publish a message
        message = {"key": "value"}
        delivered = bus.publish("test_topic", message)
        
        self.assertEqual(delivered, 1)
        
        # Receive the message
        received = sub.get_nowait()
        self.assertEqual(received, message)
    
    def test_multiple_subscribers(self):
        """Test multiple subscribers receive the same message."""
        bus = get_event_bus()
        
        sub1 = bus.subscribe("test_topic", "sub1")
        sub2 = bus.subscribe("test_topic", "sub2")
        sub3 = bus.subscribe("test_topic", "sub3")
        
        message = "test message"
        delivered = bus.publish("test_topic", message)
        
        self.assertEqual(delivered, 3)
        self.assertEqual(sub1.get_nowait(), message)
        self.assertEqual(sub2.get_nowait(), message)
        self.assertEqual(sub3.get_nowait(), message)
    
    def test_unsubscribe(self):
        """Test unsubscribe removes the subscription."""
        bus = get_event_bus()
        
        sub = bus.subscribe("test_topic", "test_sub")
        
        # Should have subscriber
        self.assertIn("test_sub", bus.get_subscribers("test_topic"))
        
        # Unsubscribe
        result = bus.unsubscribe(sub)
        self.assertTrue(result)
        
        # Should not have subscriber
        self.assertNotIn("test_sub", bus.get_subscribers("test_topic"))
        
        # Subscription should be inactive
        self.assertFalse(sub.is_active())
    
    def test_publish_to_empty_topic(self):
        """Test publishing to topic with no subscribers."""
        bus = get_event_bus()
        
        delivered = bus.publish("empty_topic", "message")
        self.assertEqual(delivered, 0)
    
    def test_queue_full_behavior(self):
        """Test behavior when subscription queue is full."""
        bus = get_event_bus()
        
        # Small queue for testing
        sub = bus.subscribe("test_topic", "test_sub", max_queue_size=3)
        
        # Fill the queue
        for i in range(5):
            bus.publish("test_topic", f"msg_{i}")
        
        # Should have dropped oldest messages
        messages = sub.get_all()
        
        # Queue should contain latest messages
        self.assertLessEqual(len(messages), 3)
    
    def test_get_with_timeout(self):
        """Test get with timeout."""
        bus = get_event_bus()
        sub = bus.subscribe("test_topic")
        
        # Should timeout on empty queue
        with self.assertRaises(queue.Empty):
            sub.get(timeout=0.1)
    
    def test_subscription_stats(self):
        """Test subscription statistics tracking."""
        bus = get_event_bus()
        sub = bus.subscribe("test_topic")
        
        # Publish some messages
        for i in range(5):
            bus.publish("test_topic", f"msg_{i}")
        
        stats = sub.stats
        self.assertEqual(stats["topic"], "test_topic")
        self.assertEqual(stats["total_messages"], 5)
        self.assertEqual(stats["pending"], 5)
        self.assertTrue(stats["active"])
    
    def test_event_bus_stats(self):
        """Test event bus statistics."""
        bus = get_event_bus()
        
        bus.subscribe("topic1", "sub1")
        bus.subscribe("topic1", "sub2")
        bus.subscribe("topic2", "sub3")
        
        bus.publish("topic1", "msg")
        bus.publish("topic2", "msg")
        
        stats = bus.get_stats()
        
        self.assertEqual(stats["total_subscribers"], 3)
        self.assertEqual(stats["total_topics"], 2)
        self.assertEqual(stats["total_published"], 2)
    
    def test_global_convenience_functions(self):
        """Test global publish/subscribe functions."""
        reset_event_bus()
        
        sub = subscribe("global_topic", "global_sub")
        delivered = publish("global_topic", "global_message")
        
        self.assertEqual(delivered, 1)
        self.assertEqual(sub.get_nowait(), "global_message")
    
    def test_thread_safety(self):
        """Test thread-safe operations."""
        bus = get_event_bus()
        results = []
        
        def publisher():
            for i in range(100):
                bus.publish("threaded_topic", i)
        
        def subscriber():
            sub = bus.subscribe("threaded_topic")
            for _ in range(100):
                try:
                    msg = sub.get(timeout=1.0)
                    results.append(msg)
                except queue.Empty:
                    break
        
        # Start subscriber first
        sub_thread = Thread(target=subscriber)
        sub_thread.start()
        
        time.sleep(0.1)  # Give subscriber time to subscribe
        
        # Start publisher
        pub_thread = Thread(target=publisher)
        pub_thread.start()
        
        pub_thread.join()
        sub_thread.join(timeout=2.0)
        
        # Should have received all messages
        self.assertEqual(len(results), 100)


class TestMarketSpecDeduplication(unittest.TestCase):
    """Tests for market spec deduplication logic."""
    
    def test_first_market_is_new(self):
        """Test that first market is always considered new."""
        dedup = MarketSpecDeduplicator()
        
        market = MarketSpec(
            market_id="market_123",
            event_id="event_1",
            slug="test-market",
            title="Test Market",
            expiry_ts_ms=int(time.time() * 1000) + 3600000,
            strike=None,
            token_yes="token_yes_123",
            token_no="token_no_123",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        self.assertTrue(dedup.is_new(market))
        self.assertEqual(dedup.change_count, 1)
    
    def test_same_market_not_new(self):
        """Test that same market ID is not considered new."""
        dedup = MarketSpecDeduplicator()
        
        market1 = MarketSpec(
            market_id="market_123",
            event_id="event_1",
            slug="test-market",
            title="Test Market",
            expiry_ts_ms=int(time.time() * 1000) + 3600000,
            strike=None,
            token_yes="token_yes_123",
            token_no="token_no_123",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        # Same market ID, different object
        market2 = MarketSpec(
            market_id="market_123",  # Same ID
            event_id="event_1",
            slug="test-market-updated",  # Different slug
            title="Test Market Updated",
            expiry_ts_ms=int(time.time() * 1000) + 3600000,
            strike=None,
            token_yes="token_yes_123",
            token_no="token_no_123",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        self.assertTrue(dedup.is_new(market1))
        self.assertFalse(dedup.is_new(market2))  # Same market_id
        self.assertEqual(dedup.change_count, 1)
    
    def test_different_market_is_new(self):
        """Test that different market ID is considered new."""
        dedup = MarketSpecDeduplicator()
        
        market1 = MarketSpec(
            market_id="market_123",
            event_id="event_1",
            slug="test-market-1",
            title="Test Market 1",
            expiry_ts_ms=int(time.time() * 1000) + 3600000,
            strike=None,
            token_yes="token_yes_1",
            token_no="token_no_1",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        market2 = MarketSpec(
            market_id="market_456",  # Different ID
            event_id="event_2",
            slug="test-market-2",
            title="Test Market 2",
            expiry_ts_ms=int(time.time() * 1000) + 3600000,
            strike=None,
            token_yes="token_yes_2",
            token_no="token_no_2",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        self.assertTrue(dedup.is_new(market1))
        self.assertTrue(dedup.is_new(market2))
        self.assertEqual(dedup.change_count, 2)
    
    def test_reset_clears_state(self):
        """Test that reset clears deduplication state."""
        dedup = MarketSpecDeduplicator()
        
        market = MarketSpec(
            market_id="market_123",
            event_id="event_1",
            slug="test-market",
            title="Test Market",
            expiry_ts_ms=int(time.time() * 1000) + 3600000,
            strike=None,
            token_yes="token_yes_123",
            token_no="token_no_123",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        dedup.is_new(market)
        self.assertEqual(dedup.change_count, 1)
        
        dedup.reset()
        self.assertEqual(dedup.change_count, 0)
        
        # Same market should be new again after reset
        self.assertTrue(dedup.is_new(market))


class TestBackoffCalculator(unittest.TestCase):
    """Tests for exponential backoff with jitter."""
    
    def test_initial_delay(self):
        """Test first delay is close to base delay."""
        backoff = BackoffCalculator(base_delay=1.0, seed=42)
        
        delay = backoff.next_delay()
        
        # Should be base_delay + jitter (0-10%)
        self.assertGreaterEqual(delay, 1.0)
        self.assertLessEqual(delay, 1.1)
    
    def test_exponential_growth(self):
        """Test delays grow exponentially."""
        backoff = BackoffCalculator(
            base_delay=1.0,
            max_delay=60.0,
            multiplier=2.0,
            jitter_factor=0.0,  # No jitter for predictable test
            seed=42
        )
        
        delays = [backoff.next_delay() for _ in range(5)]
        
        # Without jitter: 1, 2, 4, 8, 16
        self.assertAlmostEqual(delays[0], 1.0, places=5)
        self.assertAlmostEqual(delays[1], 2.0, places=5)
        self.assertAlmostEqual(delays[2], 4.0, places=5)
        self.assertAlmostEqual(delays[3], 8.0, places=5)
        self.assertAlmostEqual(delays[4], 16.0, places=5)
    
    def test_max_delay_cap(self):
        """Test delay is capped at max_delay."""
        backoff = BackoffCalculator(
            base_delay=1.0,
            max_delay=10.0,
            multiplier=2.0,
            jitter_factor=0.0,
            seed=42
        )
        
        # Get many delays
        delays = [backoff.next_delay() for _ in range(10)]
        
        # All delays should be <= max_delay
        for delay in delays:
            self.assertLessEqual(delay, 10.0)
    
    def test_deterministic_with_seed(self):
        """Test that same seed produces same sequence."""
        backoff1 = BackoffCalculator(base_delay=1.0, seed=12345)
        backoff2 = BackoffCalculator(base_delay=1.0, seed=12345)
        
        delays1 = [backoff1.next_delay() for _ in range(5)]
        delays2 = [backoff2.next_delay() for _ in range(5)]
        
        self.assertEqual(delays1, delays2)
    
    def test_reset_restarts_sequence(self):
        """Test that reset restarts the backoff sequence."""
        backoff = BackoffCalculator(
            base_delay=1.0,
            multiplier=2.0,
            jitter_factor=0.0,
            seed=42
        )
        
        # Get some delays
        backoff.next_delay()
        backoff.next_delay()
        self.assertEqual(backoff.attempt_count, 2)
        
        # Reset
        backoff.reset()
        self.assertEqual(backoff.attempt_count, 0)
        
        # First delay after reset should be base_delay
        delay = backoff.next_delay()
        self.assertAlmostEqual(delay, 1.0, places=5)
    
    def test_jitter_adds_randomness(self):
        """Test that jitter adds randomness within bounds."""
        # Same seed but with jitter
        backoff1 = BackoffCalculator(
            base_delay=10.0,
            jitter_factor=0.1,
            seed=42
        )
        
        backoff2 = BackoffCalculator(
            base_delay=10.0,
            jitter_factor=0.1,
            seed=43  # Different seed
        )
        
        delay1 = backoff1.next_delay()
        delay2 = backoff2.next_delay()
        
        # Both should be in range [10.0, 11.0]
        self.assertGreaterEqual(delay1, 10.0)
        self.assertLessEqual(delay1, 11.0)
        self.assertGreaterEqual(delay2, 10.0)
        self.assertLessEqual(delay2, 11.0)
        
        # Should be different due to different seeds
        self.assertNotEqual(delay1, delay2)


class TestDataModels(unittest.TestCase):
    """Tests for data model serialization."""
    
    def test_price_tick_to_dict(self):
        """Test PriceTick serialization."""
        tick = PriceTick(
            ts_ms=1234567890000,
            symbol="BTCUSDT",
            bid=50000.0,
            ask=50001.0,
            mid=50000.5,
            source=SourceType.BINANCE
        )
        
        d = tick.to_dict()
        
        self.assertEqual(d["type"], "price_tick")
        self.assertEqual(d["ts_ms"], 1234567890000)
        self.assertEqual(d["symbol"], "BTCUSDT")
        self.assertEqual(d["bid"], 50000.0)
        self.assertEqual(d["ask"], 50001.0)
        self.assertEqual(d["mid"], 50000.5)
        self.assertEqual(d["source"], "binance")
        
        # Should be JSON serializable
        json_str = json.dumps(d)
        self.assertIsInstance(json_str, str)
    
    def test_market_spec_to_dict(self):
        """Test MarketSpec serialization."""
        market = MarketSpec(
            market_id="condition_123",
            event_id="event_456",
            slug="btc-15min-market",
            title="Will BTC be above 50000?",
            expiry_ts_ms=1234567890000,
            strike=50000.0,
            token_yes="token_yes_abc",
            token_no="token_no_xyz",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True,
            outcomes=("Yes", "No")
        )
        
        d = market.to_dict()
        
        self.assertEqual(d["type"], "market_spec")
        self.assertEqual(d["market_id"], "condition_123")
        self.assertEqual(d["strike"], 50000.0)
        self.assertEqual(d["status"], "active")
        self.assertEqual(d["outcomes"], ["Yes", "No"])
        
        # Should be JSON serializable
        json_str = json.dumps(d)
        self.assertIsInstance(json_str, str)
    
    def test_market_price_tick_to_dict(self):
        """Test MarketPriceTick serialization."""
        tick = MarketPriceTick(
            ts_ms=1234567890000,
            market_id="market_123",
            token_id="token_abc",
            price=0.65,
            side=Side.BUY,
            source=SourceType.POLYMARKET_WS
        )
        
        d = tick.to_dict()
        
        self.assertEqual(d["type"], "market_price_tick")
        self.assertEqual(d["price"], 0.65)
        self.assertEqual(d["side"], "buy")
        self.assertEqual(d["source"], "polymarket_ws")
    
    def test_connector_health_to_dict(self):
        """Test ConnectorHealth serialization."""
        health = ConnectorHealth(
            name="test_connector",
            healthy=True,
            last_heartbeat_ts_ms=1234567890000,
            last_error=None,
            reconnect_count=3,
            connected=True,
            last_message_ts_ms=1234567889000
        )
        
        d = health.to_dict()
        
        self.assertEqual(d["type"], "connector_health")
        self.assertEqual(d["name"], "test_connector")
        self.assertTrue(d["healthy"])
        self.assertEqual(d["reconnect_count"], 3)
    
    def test_health_event_to_dict(self):
        """Test HealthEvent serialization."""
        event = HealthEvent(
            ts_ms=1234567890000,
            connector_name="test_connector",
            event_type="connected",
            message="Successfully connected",
            details={"endpoint": "wss://example.com"}
        )
        
        d = event.to_dict()
        
        self.assertEqual(d["type"], "health_event")
        self.assertEqual(d["event_type"], "connected")
        self.assertEqual(d["details"]["endpoint"], "wss://example.com")
    
    def test_price_tick_from_binance(self):
        """Test creating PriceTick from Binance bookTicker."""
        binance_msg = {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "50000.00",
            "B": "1.5",
            "a": "50001.00",
            "A": "2.0"
        }
        
        tick = PriceTick.from_binance_book_ticker(binance_msg)
        
        self.assertEqual(tick.symbol, "BTCUSDT")
        self.assertEqual(tick.bid, 50000.0)
        self.assertEqual(tick.ask, 50001.0)
        self.assertEqual(tick.mid, 50000.5)
        self.assertEqual(tick.source, SourceType.BINANCE)
    
    def test_market_spec_equality(self):
        """Test MarketSpec equality based on market_id."""
        market1 = MarketSpec(
            market_id="same_id",
            event_id="event_1",
            slug="slug-1",
            title="Title 1",
            expiry_ts_ms=1234567890000,
            strike=None,
            token_yes="yes_1",
            token_no="no_1",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        market2 = MarketSpec(
            market_id="same_id",
            event_id="event_2",  # Different
            slug="slug-2",      # Different
            title="Title 2",    # Different
            expiry_ts_ms=9999999999999,  # Different
            strike=None,
            token_yes="yes_2",  # Different
            token_no="no_2",    # Different
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        # Should be equal because market_id is the same
        self.assertEqual(market1, market2)
        self.assertEqual(hash(market1), hash(market2))
    
    def test_market_spec_get_token_ids(self):
        """Test MarketSpec.get_token_ids()."""
        market = MarketSpec(
            market_id="market_123",
            event_id="event_1",
            slug="test",
            title="Test",
            expiry_ts_ms=0,
            strike=None,
            token_yes="token_yes_abc",
            token_no="token_no_xyz",
            status=MarketStatus.ACTIVE,
            is_active=True,
            accepts_orders=True
        )
        
        tokens = market.get_token_ids()
        
        self.assertEqual(tokens, ("token_yes_abc", "token_no_xyz"))


class TestMarketSpecParsing(unittest.TestCase):
    """Tests for parsing MarketSpec from Gamma API responses."""
    
    def test_parse_valid_market(self):
        """Test parsing a valid market response."""
        gamma_response = {
            "id": "12345",
            "conditionId": "condition_abc",
            "question": "Will BTC be above $50,000?",
            "slug": "btc-50k-15min",
            "endDate": "2024-01-15T12:30:00Z",
            "clobTokenIds": "token_yes_123, token_no_456",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "enableOrderBook": True,
            "outcomes": '["Yes", "No"]',
            "groupItemThreshold": "50000",
            "events": [{"id": "event_789"}]
        }
        
        market = MarketSpec.from_gamma_response(gamma_response)
        
        self.assertIsNotNone(market)
        self.assertEqual(market.market_id, "condition_abc")
        self.assertEqual(market.token_yes, "token_yes_123")
        self.assertEqual(market.token_no, "token_no_456")
        self.assertEqual(market.strike, 50000.0)
        self.assertEqual(market.status, MarketStatus.ACTIVE)
    
    def test_parse_missing_tokens(self):
        """Test parsing market with missing token IDs."""
        gamma_response = {
            "id": "12345",
            "conditionId": "condition_abc",
            "question": "Test",
            "clobTokenIds": "",  # Empty
            "active": True
        }
        
        market = MarketSpec.from_gamma_response(gamma_response)
        
        self.assertIsNone(market)
    
    def test_parse_single_token(self):
        """Test parsing market with only one token ID."""
        gamma_response = {
            "id": "12345",
            "conditionId": "condition_abc",
            "question": "Test",
            "clobTokenIds": "single_token",  # Only one token
            "active": True
        }
        
        market = MarketSpec.from_gamma_response(gamma_response)
        
        self.assertIsNone(market)


if __name__ == "__main__":
    unittest.main()
