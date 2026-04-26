"""Structured logging configuration.

Provides structured JSON logging for production and pretty console output for development.
"""

import logging
import os
import sys

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for the pipeline.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )

    # Determine if production (JSON) or development (pretty console)
    is_prod = os.environ.get("ENVIRONMENT", "development").lower() == "production"
    renderer = (
        structlog.processors.JSONRenderer()
        if is_prod
        else structlog.dev.ConsoleRenderer()
    )

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            renderer,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):
    """Get a structured logger instance.

    Args:
        name: Logger name, typically __name__

    Returns:
        structlog BoundLogger instance
    """
    return structlog.get_logger(name)