import logging

# Logging config
logging.basicConfig(
    format='%(asctime)s- %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
LOGGER = logging.getLogger()