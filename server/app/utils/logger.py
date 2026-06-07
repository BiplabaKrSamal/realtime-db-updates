import json
import logging
import sys
from datetime import datetime, timezone


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": datetime.now(timezone.utc).isoformat(), "level": record.levelname, "message": record.getMessage(), "module": record.module}
        if hasattr(record, "extra"):
            payload.update(record.extra)
        return json.dumps(payload)


class PrettyFormatter(logging.Formatter):
    COLORS = {"DEBUG": "\x1b[34m", "INFO": "\x1b[32m", "WARNING": "\x1b[33m", "ERROR": "\x1b[31m", "CRITICAL": "\x1b[35m"}
    RESET  = "\x1b[0m"

    def format(self, record: logging.LogRecord) -> str:
        color  = self.COLORS.get(record.levelname, "")
        ts     = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        level  = f"{color}{record.levelname:<8}{self.RESET}"
        extra  = ("  " + "  ".join(f"{k}={v}" for k, v in record.extra.items())) if hasattr(record, "extra") else ""
        return f"{ts}  {level}  {record.getMessage()}{extra}"


def _build_logger(name: str = "apt") -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(PrettyFormatter())
        log.addHandler(handler)
    log.propagate = False
    return log


logger = _build_logger()
