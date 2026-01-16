from django.core.management.base import NoArgsCommand
from time import sleep, time
from django_bitcoin.utils import bitcoind
from django_bitcoin.models import BitcoinAddress, DepositTransaction
from django_bitcoin.tasks import query_transactions
from django.conf import settings
from decimal import Decimal
import datetime

RUN_TIME_SECONDS = 60


class Command(NoArgsCommand):
    help = """Executes query_transactions task
"""

    def handle_noargs(self, **options):
        query_transactions()