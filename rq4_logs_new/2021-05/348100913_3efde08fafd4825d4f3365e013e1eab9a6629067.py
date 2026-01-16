from django.core.management.base import BaseCommand, CommandError

from django.contrib.auth.models import User
from cs111.django.models import MidtermGrade

import argparse
import csv

class Command(BaseCommand):
    help = 'Adds midterm grades from a CSV file'

    def add_arguments(self, parser):
        parser.add_argument('csv', type=argparse.FileType('r'))

    def handle(self, *args, **options):
        reader = csv.reader(options['csv'])
        for row in reader:
            username = row[0]
            grade = float(row[1])

            user = User.objects.get(username=username)
            student = user.role

            midterm_grade, created = MidtermGrade.objects.get_or_create(
                student=student,
                defaults={
                    'grade': grade,
                },
            )
            if not created:
                midterm_grade.grade = grade
                midterm_grade.save()