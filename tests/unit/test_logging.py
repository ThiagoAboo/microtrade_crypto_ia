import io
import json
import logging

from core.logging import JsonFormatter, LogCategory, configure_logging


def test_json_formatter_includes_latency_field() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())

    logger = logging.getLogger("tests.logging")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(logging.INFO)

    logger.info(
        "health check completed",
        extra={"category": LogCategory.SYSTEM.value, "latency_ms": 12.5},
    )

    payload = json.loads(stream.getvalue())
    assert payload["category"] == "SYSTEM"
    assert payload["latency_ms"] == 12.5
    assert payload["message"] == "health check completed"


def test_configure_logging_preserves_existing_handlers() -> None:
    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    root_logger.handlers.clear()
    existing_handler = logging.StreamHandler(io.StringIO())
    root_logger.addHandler(existing_handler)

    try:
        configure_logging(level="INFO", json_logs=True)
        configure_logging(level="DEBUG", json_logs=True)

        assert existing_handler in root_logger.handlers
        microtrade_handlers = [
            handler
            for handler in root_logger.handlers
            if getattr(handler, "_microtrade_json_handler", False)
        ]
        assert len(microtrade_handlers) == 1
        assert root_logger.level == logging.DEBUG
    finally:
        root_logger.handlers.clear()
        root_logger.handlers.extend(previous_handlers)
