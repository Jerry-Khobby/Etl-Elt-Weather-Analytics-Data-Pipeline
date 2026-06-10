import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from ..config import LOG_DIR, LOG_LEVEL

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_LOG_FILE_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    formatter = _build_formatter()

    logger.addHandler(_build_console_handler(formatter))
    logger.addHandler(_build_file_handler(formatter))
    logger.propagate = False

    return logger


def _build_formatter() -> logging.Formatter:
    return logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)


def _build_console_handler(formatter: logging.Formatter) -> logging.StreamHandler:
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    return handler


def _build_file_handler(formatter: logging.Formatter) -> RotatingFileHandler:
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        log_dir / "pipeline.log",
        maxBytes=MAX_LOG_FILE_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(formatter)
    return handler
