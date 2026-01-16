#!/usr/bin/env python3
# -*- coding=utf-8 -*-

__author__ = 'skyeagle'

import tushare as ts
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from multiprocessing.dummy import Pool as ThreadPool


def storeStockList(engine):
    tableName = "stockName"
    stockList = ts.get_stock_basics()
    stockList.to_sql(tableName, engine, if_exists='append')


# ts.get_hist_data('600848')
def initSql():
    engine = create_engine('sqlite:///stockdatas.db', echo=False)
    return engine


if __name__ == "__main__":
    engine = initSql()
    storeStockList(engine)