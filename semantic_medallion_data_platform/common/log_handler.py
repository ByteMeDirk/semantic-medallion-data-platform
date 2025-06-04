"""
This module provides a simple Cloud-Native-like logger for Python applications,
allowing for customizable logging to the console and optional file output.
"""

import logging
from typing import Optional


def get_logger(
    name: str, level: int = logging.INFO, log_file: Optional[str] = None
) -> logging.Logger:
    """
    Creates a logger with a format that mimics Cloud (Cloud-Native) logs.

    This function sets up a logger instance with a specific format for timestamps,
    logger name, log level, and the log message. It supports logging to the
    console by default and can optionally write logs to a specified file.

    Args:
        name (str): The name of the logger. This name is included in each log
                    message and helps differentiate logs from various parts of
                    an application.
        level (int, optional): The logging level to set for the logger.
                               Common levels include `logging.DEBUG`,
                               `logging.INFO`, `logging.WARNING`,
                               `logging.ERROR`, and `logging.CRITICAL`.
                               Defaults to `logging.INFO`.
        log_file (Optional[str], optional): The absolute or relative path to a
                                             log file. If provided, log messages
                                             will be written to this file in
                                             addition to the console. If `None`,
                                             logs will only be directed to the
                                             console. Defaults to `None`.

    Returns:
        logging.Logger: A configured `logging.Logger` instance ready for use.

    Raises:
        IOError: If there's an issue creating or writing to the specified
                 `log_file`.

    Example:
        >>> # Basic usage: logs to console
        >>> app_logger = get_logger("my_application")
        >>> app_logger.info("Application started successfully.")
        2024-06-04 08:03:52.123 - my_application - INFO - Application started successfully.

        >>> # Usage with a log file
        >>> file_logger = get_logger("data_processor", log_file="processor.log")
        >>> file_logger.warning("Potential data inconsistency detected.")
        2024-06-04 08:03:52.456 - data_processor - WARNING - Potential data inconsistency detected.
    """
    # Initialize the logger with the given name.
    logger = logging.getLogger(name)
    # Set the logging level for the logger. Messages below this level will be ignored.
    logger.setLevel(level)

    # Define the log message format to mimic Cloud-Native logs.
    # `%(asctime)s.%(msecs)03d` provides a timestamp with milliseconds.
    # `%(name)s` is the logger's name, `%(levelname)s` is the log level,
    # and `%(message)s` is the actual log content.
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Create a console handler to output log messages to `sys.stderr` by default.
    console_handler = logging.StreamHandler()
    # Apply the defined formatter to the console handler.
    console_handler.setFormatter(formatter)
    # Add the console handler to the logger, so messages are printed to the console.
    logger.addHandler(console_handler)

    # If a log file path is provided, create and add a file handler.
    if log_file:
        try:
            # Create a file handler that writes log messages to the specified file.
            file_handler = logging.FileHandler(log_file)
            # Apply the same formatter to the file handler for consistent log format.
            file_handler.setFormatter(formatter)
            # Add the file handler to the logger.
            logger.addHandler(file_handler)
        except IOError as e:
            # Log an error if the file cannot be accessed or created, and re-raise.
            # This ensures the calling application is aware of the issue.
            logger.error(f"Could not open or write to log file '{log_file}': {e}")
            raise

    # Prevent the logger from propagating messages to ancestor loggers,
    # which would otherwise result in duplicate log outputs if parent loggers
    # also have handlers configured.
    logger.propagate = False

    return logger
