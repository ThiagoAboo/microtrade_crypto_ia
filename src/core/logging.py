from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class LogCategory(StrEnum):
    SYSTEM = "SYSTEM"
    TRADE = "TRADE"
    RISK = "RISK"
    MODEL = "MODEL"
    EXECUTION = "EXECUTION"
    ERROR = "ERROR"


_RESERVED_LOG_RECORD_KEYS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)
_MICROTRADE_HANDLER_MARKER = "_microtrade_json_handler"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "category": getattr(record, "category", LogCategory.SYSTEM.value),
            "event_id": getattr(record, "event_id", None),
            "symbol": getattr(record, "symbol", None),
            "latency_ms": getattr(record, "latency_ms", None),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_KEYS or key in payload:
                continue
            payload[key] = value

        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str = "INFO", json_logs: bool = True) -> None:
    root_logger = logging.getLogger()
    formatter = JsonFormatter() if json_logs else logging.Formatter("%(message)s")

    microtrade_handlers = [
        handler
        for handler in root_logger.handlers
        if getattr(handler, _MICROTRADE_HANDLER_MARKER, False)
    ]
    if microtrade_handlers:
        for handler in microtrade_handlers:
            handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler(sys.stdout)
        setattr(handler, _MICROTRADE_HANDLER_MARKER, True)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    root_logger.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
