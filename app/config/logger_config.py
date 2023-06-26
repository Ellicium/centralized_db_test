import logging
from fastapi.logger import logger

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