#!/usr/bin/python
import SqlDB

# - db interface class file
# - fluency on select, update, insert, and delete
# - correctly formats
# - able to print sql before executing


class Sql(SqlDB.SqlDB):
    def __init__(self, **kwargs):
        self._sql_ = None
        # defaults to select
        self._selector_type = 'select'
        super(Sql, self).__init__(**kwargs)

    @property
    def sql(self):
        """I am the SQL String formatted for MySQL"""
        return self._sql_

    def __str__(self):
        self._sql_ = self._compile(False)
        return self._sql_

    def select(self, table=None, fields=None, where=None, args=None):
        self._selector_type = 'select'
        self.reset()
        if table is None:
            return self

        # now contstruct from the raw select
        return self

    def update(self):
        self._selector_type = 'update'
        """do something"""

    def insert(self):
        self._selector_type = 'insert'
        """do something"""

    def delete(self):
        self._selector_type = 'delete'
        """do something"""

    def all(self):
        # override the inherited all from the select function
        if self._selector_type is not 'select':
            raise AttributeError('Invalid function call on '+self._selector_type)
        else:
            return super(Sql, self).all()