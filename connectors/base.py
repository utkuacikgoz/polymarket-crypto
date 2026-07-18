"""
Base connector class with common functionality.

Provides the interface and shared logic for all connectors.
"""

import random
from abc import ABC, abstractmethod
from threading import Event, Lock, Thread

from logging_utils import ConnectorLogger, get_logger
from models import ConnectorHealth, HealthEvent, current_ts_ms
from pubsub import TOPIC_HEALTH, publish


class BackoffCalculator:
    """
    Exponential backoff calculator with jitter.

    Implements exponential backoff with random jitter for reconnection attempts.
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter_factor: float = 0.1,
        seed: int | None = None
    ):
        """
        Initialize the backoff calculator.

        Args:
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
            multiplier: Multiplier for exponential growth
            jitter_factor: Factor for random jitter (0-1)
            seed: Random seed for deterministic testing
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter_factor = jitter_factor
        self._rng = random.Random(seed)
        self._attempt = 0

    def next_delay(self) -> float:
        """
        Calculate the next backoff delay.

        Returns:
            Delay in seconds
        """
        # Calculate exponential delay
        delay = self.base_delay * (self.multiplier ** self._attempt)
        delay = min(delay, self.max_delay)

        # Add jitter
        jitter = delay * self.jitter_factor * self._rng.random()
        delay = delay + jitter

        self._attempt += 1
        return delay

    def reset(self) -> None:
        """Reset the backoff counter."""
        self._attempt = 0

    @property
    def attempt_count(self) -> int:
        """Get current attempt count."""
        return self._attempt


class BaseConnector(ABC):
    """
    Abstract base class for all connectors.

    Provides:
    - Thread lifecycle management (start/stop)
    - Health monitoring
    - Reconnection logic with exponential backoff
    - Event emission via pubsub
    """

    def __init__(
        self,
        name: str,
        reconnect_base_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
    ):
        """
        Initialize the connector.

        Args:
            name: Unique name for this connector
            reconnect_base_delay: Base delay for reconnection backoff
            reconnect_max_delay: Maximum delay for reconnection backoff
        """
        self.name = name
        self._logger = get_logger(name)

        # Thread management
        self._thread: Thread | None = None
        self._stop_event = Event()
        self._started = False
        self._lock = Lock()

        # Health tracking
        self._connected = False
        self._healthy = False
        self._last_heartbeat_ts_ms: int = 0
        self._last_message_ts_ms: int = 0
        self._last_error: str | None = None
        self._reconnect_count = 0

        # Backoff calculator
        self._backoff = BackoffCalculator(
            base_delay=reconnect_base_delay,
            max_delay=reconnect_max_delay
        )

    @property
    def logger(self) -> ConnectorLogger:
        """Get the connector's logger."""
        return self._logger

    def start(self) -> None:
        """
        Start the connector thread.

        Raises:
            RuntimeError: If already started
        """
        with self._lock:
            if self._started:
                raise RuntimeError(f"Connector {self.name} already started")

            self._stop_event.clear()
            self._thread = Thread(target=self._run_loop, name=f"connector-{self.name}", daemon=True)
            self._thread.start()
            self._started = True
            self._logger.info("Connector started")

    def stop(self, timeout: float = 5.0) -> None:
        """
        Stop the connector thread gracefully.

        Args:
            timeout: Maximum time to wait for thread to stop
        """
        with self._lock:
            if not self._started:
                return

            self._stop_event.set()
            self._started = False

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                self._logger.warning(f"Thread did not stop within {timeout}s")

        self._thread = None
        self._connected = False
        self._healthy = False
        self._logger.info("Connector stopped")

    def is_healthy(self) -> bool:
        """Check if the connector is healthy."""
        return self._healthy and self._connected

    def last_heartbeat(self) -> int:
        """Get timestamp of last heartbeat in milliseconds."""
        return self._last_heartbeat_ts_ms

    def get_health(self) -> ConnectorHealth:
        """Get detailed health status."""
        return ConnectorHealth(
            name=self.name,
            healthy=self.is_healthy(),
            last_heartbeat_ts_ms=self._last_heartbeat_ts_ms,
            last_error=self._last_error,
            reconnect_count=self._reconnect_count,
            connected=self._connected,
            last_message_ts_ms=self._last_message_ts_ms
        )

    def _emit_health_event(self, event_type: str, message: str, **details) -> None:
        """Emit a health event to the pubsub bus."""
        event = HealthEvent(
            ts_ms=current_ts_ms(),
            connector_name=self.name,
            event_type=event_type,
            message=message,
            details=details if details else None
        )
        publish(TOPIC_HEALTH, event)

    def _update_heartbeat(self) -> None:
        """Update the last heartbeat timestamp."""
        self._last_heartbeat_ts_ms = current_ts_ms()
        self._healthy = True

    def _update_last_message(self) -> None:
        """Update the last message timestamp."""
        self._last_message_ts_ms = current_ts_ms()

    def _set_connected(self, connected: bool) -> None:
        """Update connection state."""
        was_connected = self._connected
        self._connected = connected

        if connected and not was_connected:
            self._backoff.reset()
            self._update_heartbeat()
            self._emit_health_event("connected", f"{self.name} connected")
            self._logger.connected(f"{self.name}")
        elif not connected and was_connected:
            self._healthy = False
            self._emit_health_event("disconnected", f"{self.name} disconnected")
            self._logger.disconnected()

    def _set_error(self, error: str) -> None:
        """Record an error."""
        self._last_error = error
        self._healthy = False
        self._emit_health_event("error", error)
        self._logger.error(error)

    def _should_stop(self) -> bool:
        """Check if the connector should stop."""
        return self._stop_event.is_set()

    def _wait(self, seconds: float) -> bool:
        """
        Wait for a specified time or until stop is requested.

        Returns:
            True if should continue, False if should stop
        """
        return not self._stop_event.wait(timeout=seconds)

    def _run_loop(self) -> None:
        """
        Main thread loop with reconnection logic.

        Calls _connect() and handles reconnection on failure.
        """
        while not self._should_stop():
            try:
                # Run the connection
                self._connect()
            except Exception as e:
                self._set_error(f"Connection error: {e}")
            finally:
                self._set_connected(False)

            if not self._should_stop():
                # Calculate backoff delay
                delay = self._backoff.next_delay()
                self._reconnect_count += 1

                self._emit_health_event(
                    "reconnecting",
                    f"Reconnecting in {delay:.2f}s (attempt {self._reconnect_count})",
                    delay=delay,
                    attempt=self._reconnect_count
                )
                self._logger.reconnecting(self._reconnect_count, delay)

                # Wait before reconnecting
                if not self._wait(delay):
                    break

    @abstractmethod
    def _connect(self) -> None:
        """
        Establish and maintain the connection.

        This method should:
        1. Establish the connection
        2. Call _set_connected(True) when connected
        3. Process messages in a loop
        4. Return when disconnected or error occurs

        The base class will handle reconnection.
        """
        pass
