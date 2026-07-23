# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
import io
import sys
import queue
import logging
import inspect
import platform
import datetime
import logging.config
import logging.handlers
from fastapi_app.core import settings

LOG_LEVEL = settings.LOG_LEVEL.upper()

# import sys
#
# # 1. Define ANSI Color Escape Codes
# class ColoredFormatter(logging.Formatter):
#     # ANSI escape codes for terminal coloring
#     GREY = "\x1b[38;20m"
#     GREEN = "\x1b[32;20m"
#     YELLOW = "\x1b[33;20m"
#     RED = "\x1b[31;20m"
#     BOLD_RED = "\x1b[31;1m"
#     RESET = "\x1b[0m"
#
#     def __init__(self, fmt_str):
#         super().__init__()
#         self.fmt_str = fmt_str
#         # Map log levels to specific visual colors
#         self.FORMATS = {
#             logging.DEBUG: self.GREY + self.fmt_str + self.RESET,
#             logging.INFO: self.GREEN + self.fmt_str + self.RESET,
#             logging.WARNING: self.YELLOW + self.fmt_str + self.RESET,
#             logging.ERROR: self.RED + self.fmt_str + self.RESET,
#             logging.CRITICAL: self.BOLD_RED + self.fmt_str + self.RESET
#         }
#
#     def format(self, record):
#         log_fmt = self.FORMATS.get(record.levelno, self.GREY + self.fmt_str + self.RESET)
#         formatter = logging.Formatter(log_fmt)
#         return formatter.format(record)
#
#
# # 2. Setup your variables
# log_level = settings.LOG_LEVEL or logging.INFO
# base_formatter_str = "%(asctime)s [%(levelname)s] %(name)s: %(lineno)s | %(message)s"
# print(f"{log_level=}")
#
# # 3. Build the stdout handler using our new ColoredFormatter
# console_handler = logging.StreamHandler(sys.stdout)
# console_handler.setLevel(log_level)
# console_handler.setFormatter(ColoredFormatter(base_formatter_str))
#
# # 4. Bind it to basicConfig so nothing defaults to stderr
# logging.basicConfig(
#     level=log_level,
#     handlers=[console_handler]
# )
#
# # 5. Initialize your module loggers and dependencies
# logger = logging.getLogger(__name__)
# logging.getLogger("aio_pika.tools").setLevel(logging.CRITICAL)
# logging.getLogger("aio_pika").setLevel(logging.WARNING)
# logging.getLogger("aiormq").setLevel(logging.WARNING)
#
# # --- Test Outputs ---
# logger.debug("This is an debug message (Grey).")
# logger.info("This is an info message (Green).")
# logger.warning("This is a warning message (Yellow)!")
# logger.error("This is a error message (Red)!")
# logger.critical("This is a critical message (Bold Red)!!")


# Old Approach -- Writing directly to the console/file
'''
# ==========================================================================================
# Do we need to set the logging level in multipl areas?
# -- Yes, When you call logger.info("Message"), the log message travels through two gates:
# ==========================================================================================
#       [ Logger Gate ]  --->  [ Handler Gate ]  --->  [ Destination ]
#       (First Filter)          (Second Filter)         (Console or File)
# ==========================================================================================
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "console": {
            "format": "{levelname} {message}",
            "style": "{",
        },
        "file": {
            "format": "{asctime} [{levelname}] {filename} - {funcName}: {lineno} | {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "level": LOG_LEVEL,
            "class": "logging.StreamHandler",
            "formatter": "file",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "level": LOG_LEVEL,
            "class": "logging.FileHandler",
            "formatter": "file",
            "filename": get_log_file_path()
        },
    },
    "loggers": {
        "console": {
            "handlers": ["console"],
            "propagate": True,
        },
        "file": {
            "handlers": ["file"],
            "level": LOG_LEVEL
        },
        "console_file": {
            "handlers": ["console", "file"],
            "level": LOG_LEVEL
        },
    },
}


logging.config.dictConfig(LOGGING)
logger = logging.getLogger('console_file')
'''

# ── Actual handlers (run in listener thread — no file locking issues) ──────────
_log_queue = queue.Queue(maxsize=-1)  # unbounded — adjust if memory is concern
_file_handler = logging.FileHandler(settings.LOG_FILE_PATH, encoding="utf-8")
_file_handler.setLevel(LOG_LEVEL)
_file_handler.setFormatter(logging.Formatter(
    # fmt="{asctime} [{levelname}] {filename} - {funcName}: {lineno} | {message}",
    fmt="{message}",
    style="{",
))
_rotating_file_max_bytes = 1024 * 1024 * 3
_rotating_file_backup_count = 5
_rotating_file_handler = logging.handlers.RotatingFileHandler(
    settings.LOG_FILE_PATH,
    maxBytes=_rotating_file_max_bytes,
    backupCount=_rotating_file_backup_count,
    encoding="utf-8"
)
_rotating_file_handler.setLevel(LOG_LEVEL)
_rotating_file_handler.setFormatter(logging.Formatter(
    fmt="{message}",
    style="{",
))
if str(platform.platform()).lower() == 'Windows':
    utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    _console_handler = logging.StreamHandler(stream=utf8_stdout)
else:
    _console_handler = logging.StreamHandler(stream=sys.stdout)

_console_handler.setLevel(LOG_LEVEL)
_console_handler.setFormatter(logging.Formatter(
    # fmt="{asctime} [{levelname}] {filename} - {funcName}: {lineno} | {message}",
    fmt="{message}",
    style="{",
))

# ── QueueListener — single thread handles all actual writes ───────────────────
# All workers enqueue logs → listener dequeues → writes to file/console
# No race conditions — single writer regardless of worker count
_listener = logging.handlers.QueueListener(
    _log_queue,
    _rotating_file_handler,
    _console_handler,
    respect_handler_level=True,  # honours handler-level filters
)

# ── Logging config — workers only write to queue (non-blocking) ───────────────
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "file": {
            "format": "{asctime} [{levelname}] {filename} - {funcName}: {lineno} | {message}",
            "style": "{",
        },
    },
    "handlers": {
        "queue": {
            "class": "logging.handlers.QueueHandler",
            "queue": _log_queue,  # all workers push here — non-blocking
        },
    },
    "loggers": {
        "console_file": {
            "handlers": ["queue"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
    },
    "root": {
        "handlers": ["queue"],
        "level": "WARNING",
    },
}

logging.config.dictConfig(LOGGING)


# ── Start listener — must be called once at app startup ───────────────────────
def start_listener() -> None:
    """Call this in lifespan startup — before any logging happens."""
    _listener.start()
    logging.getLogger("console_file").info("📋 QueueListener started")


def stop_listener() -> None:
    """Call this in lifespan shutdown — flushes queue before exit."""
    _listener.stop()


# ── Logger factory — drop-in replacement for logging.getLogger ────────────────
def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


# _logger = get_logger("console_file")

class Logger(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._logger = get_logger("console_file")
        return cls._instance

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self

    def get_msg_context(self, msg_from, msg, log_level, stack_info):
        current_stack_info = stack_info[2]
        file, fun, lineno = current_stack_info.filename, current_stack_info.function, current_stack_info.lineno
        file = file.rsplit(settings.ROOT_DIR, 1)[1]
        dt = datetime.datetime.now().strftime(settings.LOG_DATETIME_FMT)
        msg = f"{msg_from} [{log_level}] {dt} | .{file} - {fun} : {lineno} | {msg}"
        return msg

    def debug(self, msg, msg_from=None):
        self.put_in_queue(msg_from=msg_from, msg=msg, log_level="debug")

    def info(self, msg, msg_from=None):
        self.put_in_queue(msg_from=msg_from, msg=msg, log_level="info")

    def warning(self, msg, msg_from=None):
        self.put_in_queue(msg_from=msg_from, msg=msg, log_level="warning")

    def warn(self, msg, msg_from=None):
        self.put_in_queue(msg_from=msg_from, msg=msg, log_level="warning")

    def error(self, msg, msg_from=None):
        self.put_in_queue(msg_from=msg_from, msg=msg, log_level="error")

    def critical(self, msg, msg_from=None):
        self.put_in_queue(msg_from=msg_from, msg=msg, log_level="critical")

    def put_in_queue(self, msg_from, msg, log_level):
        if hasattr(self._instance._logger, log_level):
            msg = self.get_msg_context(msg_from, msg, log_level.upper(), stack_info=inspect.stack())
            getattr(self._instance._logger, log_level)(msg)
        else:
            print(f"Direct Print, not log_level in logger | {log_level}: {msg}")


# ── Default logger — same as before ──────────────────────────────────────────
# logger = logging.getLogger("console_file")
# logger = get_logger("console_file")
logger = Logger()
