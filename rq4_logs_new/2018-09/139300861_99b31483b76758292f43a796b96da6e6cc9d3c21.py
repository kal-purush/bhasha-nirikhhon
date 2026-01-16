# -*- coding: utf-8 -*-
from dbconnect import dbBase
from datetime import date, time


class ReadEvents(dbBase):
    def allEvents(self):
        sql = 'SELECT day, doortime, showtime, closetime, location FROM event ORDER BY showtime, day DESC'
        result = self.get_dict_resultset(sql)
        return self.replacementTypeDateToString(result)

    def replacementTypeDateToString(self, results):
        string_result = []
        for result in results:
            for k, v in result.items():
                result[k] = v if isinstance(v, str) else self.decodeToFomat(v)
            string_result.append(result)
        return string_result

    def decodeToFomat(self, mix):
        if isinstance(mix, date):
            return mix.strftime('%Y/%m/%d')
        if isinstance(mix, time):
            return mix.strftime('%H/%M/%S')
        if mix is None:
            return mix