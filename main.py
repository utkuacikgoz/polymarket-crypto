#!/usr/bin/env python3
"""
Main entry point – wires together configuration, orchestrator, and strategies.

Usage:
    python main.py
"""

import signal
import sys

from config import AppConfig
from connector_orchestrator import ConnectorOrchestrator
from strategy import LoggingStrategy
from logging_utils import get_logger

logger = get_logger("main")


def main() -> None:
    """Bootstrap the system: config → strategies → orchestrator → run."""

    # 1. Load configuration
    config = AppConfig.from_env()

    # 2. Create strategies
    #    Replace LoggingStrategy with your option-pricing strategy later.
    strategies = [
        LoggingStrategy(),
        # OptionPricingStrategy(...),
    ]

    # 3. Create orchestrator with strategies attached
    orchestrator = ConnectorOrchestrator(config=config, strategies=strategies)

    # 4. Run until interrupted (handles SIGINT / SIGTERM internally)
    orchestrator.run_forever()


if __name__ == "__main__":
    main()
