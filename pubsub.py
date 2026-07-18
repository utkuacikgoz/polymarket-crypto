"""
Thread-safe publish/subscribe event bus.

Provides a simple pub/sub mechanism using queues for loose coupling
between connectors and consumers.
"""

import queue
import time
from collections import defaultdict
from threading import Lock, RLock
from typing import Any

from logging_utils import get_logger

logger = get_logger("pubsub")


# Standard topic names
TOPIC_BINANCE_TICKS = "binance_ticks"
TOPIC_MARKET_SPEC = "market_spec"
TOPIC_MARKET_EXPIRED = "market_expired"
TOPIC_POLYMARKET_PRICES = "polymarket_prices"
TOPIC_CHAINLINK_PRICES = "chainlink_prices"
TOPIC_HEALTH = "health"


class Subscription:
    """
    A subscription to a topic.

    Holds a queue that receives published messages.
    """

    def __init__(self, topic: str, subscriber_id: str, max_size: int = 1000):
        """
        Create a new subscription.

        Args:
            topic: Topic name
            subscriber_id: Unique identifier for this subscriber
            max_size: Maximum queue size (oldest messages dropped when full)
        """
        self.topic = topic
        self.subscriber_id = subscriber_id
        self.queue: queue.Queue = queue.Queue(maxsize=max_size)
        self._active = True
        self._created_at = time.time()
        self._message_count = 0
        self._dropped_count = 0

    def get(self, timeout: float | None = None) -> Any:
        """
        Get next message from the subscription.

        Args:
            timeout: Timeout in seconds (None for blocking)

        Returns:
            The next message, or raises queue.Empty on timeout
        """
        return self.queue.get(timeout=timeout)

    def get_nowait(self) -> Any:
        """Get next message without blocking."""
        return self.queue.get_nowait()

    def get_all(self) -> list[Any]:
        """Get all available messages without blocking."""
        messages = []
        while True:
            try:
                messages.append(self.queue.get_nowait())
            except queue.Empty:
                break
        return messages

    def put(self, message: Any) -> bool:
        """
        Put a message in the subscription queue.

        Returns True if successful, False if dropped due to full queue.
        """
        try:
            self.queue.put_nowait(message)
            self._message_count += 1
            return True
        except queue.Full:
            # Drop oldest message and retry
            try:
                self.queue.get_nowait()
                self._dropped_count += 1
            except queue.Empty:
                pass
            try:
                self.queue.put_nowait(message)
                self._message_count += 1
                return True
            except queue.Full:
                self._dropped_count += 1
                return False

    def is_active(self) -> bool:
        """Check if subscription is still active."""
        return self._active

    def deactivate(self) -> None:
        """Deactivate this subscription."""
        self._active = False

    @property
    def pending_count(self) -> int:
        """Get number of pending messages."""
        return self.queue.qsize()

    @property
    def stats(self) -> dict[str, Any]:
        """Get subscription statistics."""
        return {
            "topic": self.topic,
            "subscriber_id": self.subscriber_id,
            "active": self._active,
            "pending": self.pending_count,
            "total_messages": self._message_count,
            "dropped_messages": self._dropped_count,
            "age_seconds": time.time() - self._created_at,
        }


class EventBus:
    """
    Thread-safe event bus for pub/sub messaging.

    Features:
    - Multiple subscribers per topic
    - Non-blocking publish
    - Subscription management
    - Message filtering via predicates
    """

    def __init__(self):
        """Initialize the event bus."""
        self._lock = RLock()
        self._subscriptions: dict[str, dict[str, Subscription]] = defaultdict(dict)
        self._subscriber_counter = 0
        self._publish_count = 0

    def subscribe(
        self,
        topic: str,
        subscriber_id: str | None = None,
        max_queue_size: int = 1000
    ) -> Subscription:
        """
        Subscribe to a topic.

        Args:
            topic: Topic name to subscribe to
            subscriber_id: Optional subscriber identifier (auto-generated if None)
            max_queue_size: Maximum number of messages to queue

        Returns:
            Subscription object with a queue for receiving messages
        """
        with self._lock:
            if subscriber_id is None:
                self._subscriber_counter += 1
                subscriber_id = f"sub_{self._subscriber_counter}"

            subscription = Subscription(topic, subscriber_id, max_queue_size)
            self._subscriptions[topic][subscriber_id] = subscription

            logger.debug(f"New subscription: {subscriber_id} -> {topic}")

            return subscription

    def unsubscribe(self, subscription: Subscription) -> bool:
        """
        Unsubscribe from a topic.

        Args:
            subscription: The subscription to remove

        Returns:
            True if successfully unsubscribed, False if not found
        """
        with self._lock:
            topic_subs = self._subscriptions.get(subscription.topic, {})
            if subscription.subscriber_id in topic_subs:
                subscription.deactivate()
                del topic_subs[subscription.subscriber_id]
                logger.debug(f"Unsubscribed: {subscription.subscriber_id} from {subscription.topic}")
                return True
            return False

    def publish(self, topic: str, message: Any) -> int:
        """
        Publish a message to a topic.

        Args:
            topic: Topic name to publish to
            message: Message to publish (any serializable object)

        Returns:
            Number of subscribers that received the message
        """
        with self._lock:
            subscriptions = list(self._subscriptions.get(topic, {}).values())

        delivered = 0
        for sub in subscriptions:
            if sub.is_active():
                if sub.put(message):
                    delivered += 1

        self._publish_count += 1

        return delivered

    def get_subscribers(self, topic: str) -> list[str]:
        """Get list of subscriber IDs for a topic."""
        with self._lock:
            return list(self._subscriptions.get(topic, {}).keys())

    def get_topics(self) -> list[str]:
        """Get list of all topics with subscribers."""
        with self._lock:
            return [t for t, subs in self._subscriptions.items() if subs]

    def get_stats(self) -> dict[str, Any]:
        """Get event bus statistics."""
        with self._lock:
            topic_stats = {}
            total_subs = 0
            for topic, subs in self._subscriptions.items():
                active_subs = [s for s in subs.values() if s.is_active()]
                topic_stats[topic] = {
                    "subscriber_count": len(active_subs),
                    "total_pending": sum(s.pending_count for s in active_subs),
                }
                total_subs += len(active_subs)

            return {
                "total_subscribers": total_subs,
                "total_topics": len(topic_stats),
                "total_published": self._publish_count,
                "topics": topic_stats,
            }

    def clear_topic(self, topic: str) -> int:
        """
        Remove all subscriptions for a topic.

        Returns:
            Number of subscriptions removed
        """
        with self._lock:
            subs = self._subscriptions.get(topic, {})
            count = len(subs)
            for sub in subs.values():
                sub.deactivate()
            self._subscriptions[topic] = {}
            return count

    def clear_all(self) -> None:
        """Remove all subscriptions from all topics."""
        with self._lock:
            for topic in self._subscriptions:
                for sub in self._subscriptions[topic].values():
                    sub.deactivate()
            self._subscriptions.clear()


# Global event bus instance
_event_bus: EventBus | None = None
_bus_lock = Lock()


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _event_bus
    with _bus_lock:
        if _event_bus is None:
            _event_bus = EventBus()
        return _event_bus


def reset_event_bus() -> None:
    """Reset the global event bus (useful for testing)."""
    global _event_bus
    with _bus_lock:
        if _event_bus is not None:
            _event_bus.clear_all()
        _event_bus = None


# Convenience functions for common operations

def publish(topic: str, message: Any) -> int:
    """Publish a message to a topic on the global event bus."""
    return get_event_bus().publish(topic, message)


def subscribe(topic: str, subscriber_id: str | None = None) -> Subscription:
    """Subscribe to a topic on the global event bus."""
    return get_event_bus().subscribe(topic, subscriber_id)


def unsubscribe(subscription: Subscription) -> bool:
    """Unsubscribe from the global event bus."""
    return get_event_bus().unsubscribe(subscription)
