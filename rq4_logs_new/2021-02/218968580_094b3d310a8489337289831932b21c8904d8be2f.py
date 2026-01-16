import logging

from apps.fraudprediction.fraud_predict import FraudPredict
from celery import shared_task
from django.conf import settings

logger = logging.getLogger(__name__)

DEFAULT_RETRY_DELAY = 10
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 60


@shared_task(bind=True, default_retry_delay=DEFAULT_RETRY_DELAY)
def fraudpredict(self, fraudprediction_type):
    """
    Calculate fraudpredictions per type
    """

    try:
        logger.info("Started fraudpredict task")
        fraud_predict = FraudPredict()
        fraud_predict.start()
        logger.info("Ended fraudpredict task")

        return True
    except Exception as exception:
        self.retry(exc=exception)