import logging
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Optional
import sys
import traceback
IST = timezone(timedelta(hours=5, minutes=30))


def setup_logging(level: str = "INFO", log_dir: str = "logs", run_id: Optional[str] = None) -> str:
    """
    Configure logging to write to both STDOUT and a timestamped file.

    Args:
        level: Logging level name (e.g., "DEBUG", "INFO", "WARNING").
        log_dir: Directory to place log files (created if missing).
        run_id: Optional identifier (e.g., job name or ULID) to prefix the log filename.

    Returns:
        The absolute path of the created log file.
    """
    os.makedirs(log_dir, exist_ok=True)

    ts = datetime.now(IST).strftime("%Y%m%d-%H%M%S")
    filename = f"{run_id + '_' if run_id else ''}{ts}.log"
    file_path = os.path.abspath(os.path.join(log_dir, filename))

    # Reset root logger handlers to avoid duplicate logs if setup is called twice
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    # Set level on root
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Consistent formatter (timestamps will reflect system local time; message content includes tz in filename)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.captureWarnings(True)

    # STDOUT handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    # File handler
    file_handler = logging.FileHandler(file_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Prevent propagation to avoid duplicate logs in some environments
    root.propagate = False

    logging.getLogger(__name__).info("Logging initialized: level=%s, file=%s", level.upper(), file_path)
    

    return file_path


def install_excepthook(logger_name: str = __name__):
    """
    Installs a sys.excepthook that logs any uncaught exceptions at CRITICAL level with traceback.

    Args:
        logger_name: The logger name to use for exception logging.
    """


    logger = logging.getLogger(logger_name)

    def _hook(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.critical(
            "Uncaught exception",
            exc_info=(exc_type, exc_value, exc_traceback)
        )
        # Also print to stderr for immediate visibility
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)

    sys.excepthook = _hook