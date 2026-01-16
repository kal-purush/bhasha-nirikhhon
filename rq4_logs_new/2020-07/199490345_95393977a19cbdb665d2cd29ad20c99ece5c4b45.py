from django.core.management.base import BaseCommand
from openpyxl import load_workbook
# from backend.lists.models.answer import Answer
from ...models import Answer
from ...models.response import Response
import datetime
from ...models.survey import Survey
from ...models.question import Question
from user_profile.models import UserProfile


class Command(BaseCommand):
    help = 'Импорт ответов для дзд'

    def add_arguments(self, parser):
        parser.add_argument('file', type=str)

    def handle(self, *args, **options):
        wb = load_workbook(filename=options['file']).worksheets[0]
        i = 0
        for row in wb.rows:
            if i == 0:
                i += 1
                continue

            date = datetime.datetime(2020, 1, 1, 0, 0, 0, 0)
            tmp = Response(created=date,
                           updated=date,
                           survey=Survey.objects.get(pk=6),
                           user=UserProfile.objects.get(id=4))
            tmp.save()
            self.add_answer(tmp, Question.objects.get(pk=66), -1, date)
            self.add_answer(tmp, Question.objects.get(
                pk=69), "Договор заключен", date)
            self.add_answer(tmp, Question.objects.get(
                pk=67), row[5].value, date)
            self.add_answer(tmp, Question.objects.get(
                pk=68), row[6].value, date)
            self.add_answer(tmp, Question.objects.get(
                pk=65), row[8].value, date)
            # print(
            #     f'{row[1].value};{row[2].value};{row[5].value};{row[6].value};{row[8].value};Басина;01-01-2020')

    def add_answer(self, response, question, value, datetime):
        res = Answer(question=question,
                     response=response,
                     created=datetime,
                     updated=datetime,
                     body=value)
        res.save()