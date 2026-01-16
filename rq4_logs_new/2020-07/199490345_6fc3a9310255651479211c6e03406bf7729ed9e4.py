from openpyxl import Workbook
from openpyxl.styles import Protection, PatternFill
from openpyxl.styles.colors import Color
from tempfile import NamedTemporaryFile

from datetime import datetime


def create_file(query):
    wb = Workbook(write_only=True)
    stream = None
    ws = wb.create_sheet()
    # HEADER
    ws.append(create_header(query))
    row = ws.row_dimensions[1]
    p = Protection(locked=True, hidden=False)
    c = Color(indexed=22)
    f = PatternFill(fill_type='solid', start_color=c, end_color=c)
    row.fill = f
    row.protection = p

    # BODY
    body = get_data_from_query_as_lists(query)
    for data in body:
        ws.append(data)
    with NamedTemporaryFile() as tmp:
        wb.save(tmp.name)
        tmp.seek(0)
        stream = tmp.read()
        return stream


def create_header(query):
    result = ['Ссылка', 'Номер ответа', 'Дата создания', 'Почта']
    for answer in query.first().answers.all():
        result.append(answer.question.text)
    print(result)
    return result


def get_data_from_query_as_lists(query):
    results = list()
    LINK = "http://checklist.landfinance.ru/response/"
    for q in query:
        new_piece = [
            f'{LINK}{q.id}',
            q.id,
            q.created,
            q.user.email
        ]
        for answer in q.answers.all():
            new_piece.append(answer.body)
        results.append(new_piece)
    return results