"""
Logging utilities for the market data connectors.

Provides JSONL (JSON Lines) logging with rotating file handler and structured output.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from config import LoggingConfig, get_config


class JSONLFormatter(logging.Formatter):
    """
    Formatter that outputs log records as JSON lines.
    
    Each line is a complete JSON object with consistent fields.
    """
    
    def __init__(self):
        super().__init__()
        self._lock = Lock()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON line."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "ts_ms": int(record.created * 1000),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields from the record
        # These can be passed via logger.info("msg", extra={"key": "value"})
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "exc_info", "exc_text", "thread", "threadName",
                "message", "taskName"
            ):
                try:
                    # Try to serialize the value
                    json.dumps(value)
                    log_entry[key] = value
                except (TypeError, ValueError):
                    log_entry[key] = str(value)
        
        with self._lock:
            return json.dumps(log_entry, default=str)


class ConsoleFormatter(logging.Formatter):
    """
    Colored console formatter for human-readable output.
    """
    
    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    
    def format(self, record: logging.LogRecord) -> str:
        """Format with colors for terminal output."""
        color = self.COLORS.get(record.levelname, "")
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Build the base message
        msg = f"{color}[{timestamp}] [{record.levelname:8s}] [{record.name}]{self.RESET} {record.getMessage()}"
        
        # Add extra fields if present
        extras = []
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "exc_info", "exc_text", "thread", "threadName",
                "message", "taskName"
            ):
                extras.append(f"{key}={value}")
        
        if extras:
            msg += f" | {', '.join(extras)}"
        
        # Add exception info if present
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        
        return msg


class ConnectorLogger:
    """
    Logger wrapper for connector-specific logging.
    
    Provides convenient methods for logging connector events with structured data.
    """
    
    def __init__(self, name: str, logger: logging.Logger):
        self.name = name
        self._logger = logger
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with extra fields."""
        self._logger.debug(message, extra={"connector": self.name, **kwargs})
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message with extra fields."""
        self._logger.info(message, extra={"connector": self.name, **kwargs})
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with extra fields."""
        self._logger.warning(message, extra={"connector": self.name, **kwargs})
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message with extra fields."""
        self._logger.error(message, extra={"connector": self.name, **kwargs})
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message with extra fields."""
        self._logger.critical(message, extra={"connector": self.name, **kwargs})
    
    def exception(self, message: str, **kwargs) -> None:
        """Log exception with traceback."""
        self._logger.exception(message, extra={"connector": self.name, **kwargs})
    
    def connected(self, endpoint: str, **kwargs) -> None:
        """Log connection event."""
        self.info(f"Connected to {endpoint}", event_type="connected", endpoint=endpoint, **kwargs)
    
    def disconnected(self, reason: str = "", **kwargs) -> None:
        """Log disconnection event."""
        self.warning(f"Disconnected: {reason}", event_type="disconnected", reason=reason, **kwargs)
    
    def reconnecting(self, attempt: int, delay: float, **kwargs) -> None:
        """Log reconnection attempt."""
        self.info(f"Reconnecting (attempt {attempt}, delay {delay:.2f}s)", 
                  event_type="reconnecting", attempt=attempt, delay=delay, **kwargs)
    
    def heartbeat(self, **kwargs) -> None:
        """Log heartbeat event."""
        self.debug("Heartbeat", event_type="heartbeat", **kwargs)
    
    def market_switch(self, old_market_id: Optional[str], new_market_id: str, **kwargs) -> None:
        """Log market switch event."""
        self.info(f"Switching market: {old_market_id} -> {new_market_id}",
                  event_type="market_switch", old_market_id=old_market_id, 
                  new_market_id=new_market_id, **kwargs)
    
    def tick(self, tick_data: Dict[str, Any], **kwargs) -> None:
        """Log tick data (debug level)."""
        self.debug("Tick received", event_type="tick", tick=tick_data, **kwargs)


# Module-level state
_initialized = False
_init_lock = Lock()
_loggers: Dict[str, ConnectorLogger] = {}


def setup_logging(config: Optional[LoggingConfig] = None) -> None:
    """
    Initialize the logging system.
    
    Should be called once at application startup.
    
    Args:
        config: Logging configuration. If None, loads from environment.
    """
    global _initialized
    
    with _init_lock:
        if _initialized:
            return
        
        if config is None:
            config = get_config().logging
        
        # Create log directory if needed
        log_dir = Path(config.log_path)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Get the root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers
        root_logger.handlers.clear()
        
        # Add JSONL file handler
        log_file = log_dir / config.log_file_name
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=config.max_file_size_mb * 1024 * 1024,
            backupCount=config.backup_count,
            encoding="utf-8"
        )
        file_handler.setLevel(getattr(logging, config.file_level.upper()))
        file_handler.setFormatter(JSONLFormatter())
        root_logger.addHandler(file_handler)
        
        # Add console handler if enabled
        if config.console_enabled:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, config.console_level.upper()))
            console_handler.setFormatter(ConsoleFormatter())
            root_logger.addHandler(console_handler)
        
        _initialized = True


def get_logger(name: str) -> ConnectorLogger:
    """
    Get or create a logger for a connector.
    
    Args:
        name: Name of the connector (e.g., "binance_ws", "polymarket_gamma")
        
    Returns:
        ConnectorLogger instance for the connector.
    """
    global _loggers
    
    if name not in _loggers:
        # Ensure logging is initialized
        if not _initialized:
            setup_logging()
        
        logger = logging.getLogger(f"connector.{name}")
        _loggers[name] = ConnectorLogger(name, logger)
    
    return _loggers[name]


def log_model(model: Any, logger_name: str = "data") -> None:
    """
    Log a data model to the JSONL log.
    
    Args:
        model: Data model with to_dict() method
        logger_name: Name of the logger to use
    """
    logger = get_logger(logger_name)
    if hasattr(model, "to_dict"):
        logger.debug("Data model", data=model.to_dict())
    else:
        logger.debug("Data model", data=str(model))
