import logging

formatter = logging.Formatter("[%(levelname)s@%(asctime)s][%(filename)s:%(lineno)s - %(funcName)2s() ] %(message)s")
stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
loglevel = logging.DEBUG
logger.setLevel(loglevel)
logger.addHandler(stdout_handler)
