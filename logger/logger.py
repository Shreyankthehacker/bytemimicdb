import logging
import sys

# Create a logger instance
logger = logging.getLogger("database_shrey")

# Set the lowest level of messages this logger will handle
logger.setLevel(logging.DEBUG)

# Create a console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Create a formatter for nice log output
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)

# Attach formatter to the handler
console_handler.setFormatter(formatter)

# Add the handler to the logger (but only once)
if not logger.hasHandlers():
    logger.addHandler(console_handler)
