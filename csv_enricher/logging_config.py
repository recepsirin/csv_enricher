import logging
import os
from logging.handlers import RotatingFileHandler

# Create a 'logs' directory if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

info_handler = RotatingFileHandler("logs/info.log", maxBytes=10 * 1024 * 1024, backupCount=5)
info_handler.setLevel(logging.INFO)

error_handler = RotatingFileHandler("logs/error.log", maxBytes=10 * 1024 * 1024, backupCount=5)
error_handler.setLevel(logging.ERROR)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

info_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)

logger.addHandler(info_handler)
logger.addHandler(error_handler)
