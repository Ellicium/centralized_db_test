import os
import logging
from dotenv import load_dotenv
from fastapi.logger import logger

load_dotenv()

def get_gunicorn_logger():
    gunicorn_logger = logging.getLogger('gunicorn.error')
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
    return logger

def get_uvicorn_logger():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.propagate = False
    logger = logging.getLogger(__name__)
    return logger


def get_logger():
    web_server = os.getenv("webServer")
    print(f"webServer:{web_server}")
    if web_server == "gunicorn":
        logger = get_gunicorn_logger()
    elif web_server == "uvicorn":
        logger = get_uvicorn_logger()
    return logger
    