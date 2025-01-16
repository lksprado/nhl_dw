import logging 
from logging.handlers import RotatingFileHandler


logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()

rotating_file_handler = RotatingFileHandler("app/api_logs.log", maxBytes=2000)

console_handler.setLevel(logging.WARNING)
rotating_file_handler.setLevel(logging.ERROR)

logging_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_handler.setFormatter(logging_format)
rotating_file_handler.setFormatter(logging_format)

logger.addHandler(console_handler)
logger.addHandler(rotating_file_handler)


