# -*- coding: utf-8 -*-
from django.core.management.base import BaseCommand  # , CommandError
from core.models import aTimeLogger
import csv
from datetime import datetime, timedelta
from django.core.files.storage import default_storage


class Command(BaseCommand):
    help = 'Imports aTimeLogger data from a csv file on Dropbox'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Running the aTimeLogger import'))

        self.stdout.write(self.style.SUCCESS('Processing file'))
        self.read_csv("blah")
        self.stdout.write(self.style.SUCCESS('imported file'))

    def read_csv(self, filename):
        # TODO: make permanent (maybe a selector from Dropbox for the FileConfig file_type 'SleepAs')

        if default_storage.exists('/QuantMe/report.csv'):
            with default_storage.open('/QuantMe/report.csv', 'r') as csvfile:
                source = csv.reader(csvfile)
                for row in source:
                    if len(row) < 1:
                        break
                    if row[0] == "Activity type":
                        continue  # skip the header
                    self.stdout.write(self.style.SUCCESS('row: "%s"' % row))
                    aTL, created = aTimeLogger.objects.get_or_create(
                        ActivityType=row[0],
                        Duration=row[1],
                        start_time=datetime.strptime(row[2], '%Y-%m-%d %H:%M'),
                        end_time=datetime.strptime(row[3], '%Y-%m-%d %H:%M'),
                        Comment=row[4]
                    )
                    self.stdout.write(self.style.SUCCESS('created: "%s"' % created))
                self.stdout.write(self.style.SUCCESS('completed'))
        else:
            self.style.FAILURE("file doesn't exist")
            exit