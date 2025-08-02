import json
import logging
import os
from datetime import datetime
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dj.utils import resolve_internal_dir


def get_logs_dir() -> Path:
    environ_logs_dir: str = os.environ.get("LOGS_DIR")
    if environ_logs_dir:
        log_dir: Path = Path(environ_logs_dir)
    # Determine appropriate log directory based on platform
    else:
        log_dir: Path = Path(resolve_internal_dir()) / "logs"

    # Create directory if it doesn't exist
    log_dir.mkdir(parents=True, exist_ok=True)

    return log_dir


class ColoredFormatter(logging.Formatter):
    # ANSI color codes
    COLORS: dict[str, str] = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET: str = "\033[0m"  # Reset color

    def format(self, record) -> str:
        # Get the original formatted message
        formatted: str = super().format(record)

        # Add color to the entire message if this is console output
        if hasattr(record, "console_output") and record.console_output:
            color = self.COLORS.get(record.levelname, "")
            formatted = f"{color}{formatted}{self.RESET}"

        return formatted


class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for logging."""

    def __init__(self, verbose: bool = False):
        super().__init__()
        self.verbose = verbose

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON string."""
        log_record: dict[str] = {
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


def configure_logging(
    prog_name: str,
    log_dir: str | None = None,
    enable_colors: bool = True,
    verbose: bool = False,
) -> str:
    # Set log levels based on verbose flag
    log_level: str = "DEBUG" if verbose else "INFO"
    console_level: str = "DEBUG" if verbose else "INFO"

    # Create formatters
    if enable_colors:
        if verbose:
            console_formatter = ColoredFormatter(
                "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%H:%M:%S",
            )
        else:
            console_formatter = ColoredFormatter("%(message)s")
    else:
        if verbose:
            console_formatter = logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%H:%M:%S",
            )
        else:
            console_formatter = logging.Formatter("%(message)s")

    # Create log directory if it doesn't exist
    if not log_dir:
        log_dir = get_logs_dir()

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / f"{prog_name}.log"

    # Create rotating file handler with JSON formatting
    file_handler = RotatingFileHandler(
        filename=str(log_path),
        mode="a",
        maxBytes=500 * 1024 * 1024,  # 500MB
        backupCount=5,  # Keep 5 rotated logs (total: 2.5GB max)
        encoding="utf-8",
        delay=False,
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(JsonFormatter(verbose=verbose))

    # Create console handler
    console_handler = StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)

    # Add color filtering if enabled
    if enable_colors:

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

    return str(log_path)
