#!/usr/bin/env python
# coding: utf-8
import cx_Oracle


from time import time

class ColumnHeader(object):
  def __init__(self, title):
    self.title = title.strip()
    self.summary = None

  def add_summary(self, column):
    self.summary = ColumnSummary(self, column)

  def __unicode__(self):
    return self.title

  def __str__(self):
    return self.title

class ColumnStat(object):
  def __init__(self, label, statfn, precision=2, handles_null=False):
    self.label = label
    self.statfn = statfn
    self.precision = precision
    self.handles_null = handles_null

  def __call__(self, coldata):
    self.value = round(float(self.statfn(coldata)), self.precision) if coldata else 0

  def __unicode__(self):
    return self.label

  def foo(self):
    return "foobar"

class ColumnSummary(object):
  def __init__(self, header, col):
    self._header = header
    self._stats = [
      ColumnStat("Sum", sum),
      ColumnStat("Avg", lambda x: float(sum(x)) / float(len(x))),
      ColumnStat("Min", min),
      ColumnStat("Max", max),
      ColumnStat("NUL", lambda x: int(sum(map(lambda y: 1 if y is None else 0, x))), 0, True)
    ]
    without_nulls = list(map(lambda x: 0 if x is None else x, col))

    for stat in self._stats:
      stat(col) if stat.handles_null else stat(without_nulls)

  @property
  def stats(self):
    # dict comprehensions are not supported in Python 2.6, so do this instead
    return dict((c.label, c.value) for c in self._stats)

  def __str__(self):
    return str(self._header)

class QueryResult(object):
  def __init__(self, sql, cursor, debug=False):

    self.sql = sql
    self.cursor = cursor
    duration = self.execute_query()

    self._description = cursor.description or []
    # if self.args.debug:
    #         print sql
    self._debug=debug
    self._data = [list(r) for r in cursor.fetchall()]
    self.duration = duration
    # cursor.close()
    self._headers = self._get_headers()
    self._summary = {}

  @property
  def data(self):
    return self._data or []

  @property
  def headers(self):
    return self._headers or []

  def _get_headers(self):
    return [ColumnHeader(d[0]) for d in self._description] if self._description else [ColumnHeader('--')]

  def _get_numerics(self):
    # conn = get_connection()
    if hasattr(conn.Database, "NUMBER"):
      return [ix for ix, c in enumerate(self._description) if
          hasattr(c, 'type_code') and c.type_code in conn.Database.NUMBER.values]
    elif self.data:
      d = self.data[0]
      return [ix for ix, _ in enumerate(self._description) if
          not isinstance(d[ix], six.string_types) and six.text_type(d[ix]).isnumeric()]
    return []

  def _get_transforms(self):
    transforms = dict(app_settings.EXPLORER_TRANSFORMS)
    return [(ix, transforms[str(h)]) for ix, h in enumerate(self.headers) if str(h) in transforms.keys()]

  def column(self, ix):
    return [r[ix] for r in self.data]

  def process(self):
    start_time = time()

    self.process_columns()
    self.process_rows()

    # logger.info("Explorer Query Processing took %sms." % ((time() - start_time) * 1000))

  def process_columns(self):
    for ix in self._get_numerics():
      self.headers[ix].add_summary(self.column(ix))

  def process_rows(self):
    transforms = self._get_transforms()
    if transforms:
      for r in self.data:
        for ix, t in transforms:
          r[ix] = t.format(str(r[ix]))

  def execute_query(self):
    start_time = time()
    try:
      self.cursor.execute(self.sql)
    except Exception as e:
      self.cursor.close()
      print str(e)
      raise e

    return ((time() - start_time) * 1000)

  def getcolformatstr(self, coldef):
    if coldef[1] == cx_Oracle.NUMBER:
      collength = 12;
    else:
      if coldef[2] <= 10:
        collength = coldef[2]
      elif coldef[2] > 10  and coldef[2] <= 20 :
        collength = coldef[2] - 2
      elif coldef[2] > 20  and coldef[2] <= 32 :
        collength = coldef[2] - 5
      elif coldef[2] > 32  and coldef[2] < 64:
        collength = 30
      elif coldef[2] > 64  and coldef[2] <= 128:
        collength = 30
      elif coldef[2] > 128 and coldef[2] <= 600:
        collength = 60
      else:
        collength = 32
      if coldef[2] < len(coldef[0]):
        collength = len(coldef[0])

    return collength

  def show_rows(self):

    if self._data != []:
      if self._debug:
        print ('-' * 128)
        for col in self._description:
          print ('Name: \033[1;31;40m%-20s\033[0m' % col[0]),
          print ('Type: \033[1;31;40m%10s\033[0m' % str(col[1])),
          print ('Display Size: \033[1;31;40m%5s\033[0m' % str(col[2])),
          print ('Internal Size: \033[1;31;40m%5s\033[0m' % str(col[3])),
          print ('Precision : \033[1;31;40m%5s\033[0m' % str(col[4])),
          print ('Scale : \033[1;31;40m%4s\033[0m' % str(col[5])),
          print ('Nullable : \033[1;31;40m%2s\033[0m' % str(col[6])),
          print ('Print Display : \033[1;31;40m%5s\033[0m' % str(self.getcolformatstr(col)))
        print ('-' * 128 + "\n")

      for col in self._description:
        colformatstr = ('%-' + str(self.getcolformatstr(col)) + 's')
        print(colformatstr % (col[0])),
      print('');
      for col in self._description:
        print('-' * self.getcolformatstr(col)),
      print('');

      for row in self._data:
        for i in range(len(row)):
          colformatstr = ('%-' + str(self.getcolformatstr(self._description[i])) + 's')
          if row[i] != None:
            print(colformatstr % row[i]), ;
          else:
            print(colformatstr % ''), ;
        print('')

  def show_rows_csv(self,db_info):
      for row in self._data:
        print ('"%s","%s",' % (db_info['db_ip'],db_info['db_name'])) , ;
        for i in range(len(row)):

          colformatstr = ('"%-' + 's",')
          if row[i] != None:
            print(colformatstr % row[i]), ;
          else:
            print(colformatstr % ''), ;
        print('')