# django
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

# api app
from api.models import *

# other things
from datetime import timedelta
import os

class Command(BaseCommand):
    help = 'Closes the specified poll for voting'

    def handle(self, *args, **options):

        fourteen_days_ago = timezone.now() - timedelta(days=14)
        old_matches = Match.objects.filter(created__lt=fourteen_days_ago)

        logs_path = os.path.realpath('.../logs/deletion_log.log')

        log_file = open(logs_path, 'a+')

        log_file.write(f'########################## COMMAND BEING RAN AT {timezone.now().strftime("%b %d %Y @ %H:%M:%S")} | {old_matches.count()} OBJECTS TO BE DELETED ##########################\n')

        for match in old_matches:

            days_old = timezone.now() - match.created
            created = match.created.strftime("%b %d %Y @ %H:%M:%S")
            data = f"~~~~ Match was created on {created} and is {days_old.days} days old ~~~~~\n"
            log_file.write(data)
            match.delete()

        log_file.close()



       