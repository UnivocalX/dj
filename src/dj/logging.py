import json
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

from tqdm import tqdm

from dj.utils import resolve_internal_dir


def get_logs_dir() -> str:
    environ_logs_dir: str | None = os.environ.get("LOGS_DIR")
    if not environ_logs_dir:
        environ_logs_dir = os.path.join(resolve_internal_dir(), "logs")
    return environ_logs_dir


class ColoredFormatter(logging.Formatter):
    COLORS: dict[str, str] = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET: str = "\033[0m"  # Reset color

    def format(self, record) -> str:
        formatted: str = super().format(record)
        color = self.COLORS.get(record.levelname, "")
        return f"{color}{formatted}{self.RESET}"


class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for logging."""

    def __init__(self, verbose: bool = False):
        super().__init__()
        self.verbose = verbose

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON string."""
        log_record: dict[str, str | int] = {
            "timestamp": datetime.now().isoformat(),
            "name": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }

        if self.verbose:
            log_record.update(
                {
                    "function": record.funcName,
                    "lineno": record.lineno,
                    "module": record.module,
                    "pathname": record.pathname,
                }
            )

        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


class TqdmLoggingHandler(logging.StreamHandler):
    """A logging handler that uses tqdm.write() to avoid breaking progress bars."""

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def configure_logging(
    prog_name: str,
    log_dir: str | None = None,
    plain: bool = False,
    verbose: bool = False,
) -> str:
    # Set log levels based on verbose flag
    log_level: str = "DEBUG" if verbose else "INFO"
    console_level: str = "DEBUG" if verbose else "INFO"

    # Create formatters
    if plain:
        formatter_cls = logging.Formatter
    else:
        formatter_cls = ColoredFormatter

    if verbose:
        fmt: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        datefmt: str = "%H:%M:%S"
    else:
        fmt = "%(message)s"
        datefmt = None

    if datefmt:
        console_formatter = formatter_cls(fmt, datefmt=datefmt)
    else:
        console_formatter = formatter_cls(fmt)

    # Create log directory if it doesn't exist
    if not log_dir:
        log_dir = get_logs_dir()

    # Create rotating file handler with JSON formatting
    os.makedirs(log_dir, exist_ok=True)
    log_path: str = os.path.join(log_dir, f"{prog_name}.log")
    file_handler = RotatingFileHandler(
        filename=log_path,
        mode="a",
        maxBytes=500 * 1024 * 1024,  # 500MB
        backupCount=5,  # Keep 5 rotated logs (total: 2.5GB max)
        encoding="utf-8",
        delay=False,
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(JsonFormatter(verbose=verbose))

    # Use TqdmLoggingHandler for console output
    console_handler = TqdmLoggingHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)

    # Add color filtering if enabled
    if plain:

        class ConsoleFilter(logging.Filter):
            def filter(self, record):
                record.console_output = True
                return True

        console_handler.addFilter(ConsoleFilter())

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel("DEBUG")  # Set to lowest level, handlers will filter
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Log configuration details
    logger = logging.getLogger(__name__)
    logger.debug("Logging configured")
    logger.debug(f"Verbose mode: {verbose}")
    logger.debug(f"Log file: {log_path}")

    return log_path
