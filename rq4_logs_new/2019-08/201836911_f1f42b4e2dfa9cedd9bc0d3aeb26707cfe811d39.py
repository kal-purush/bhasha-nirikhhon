#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# =====================================================================
# Copyright (C) 2018-2019 by Owl Data
# author: Danny, Destiny

# =====================================================================
import pandas as pd

# --------------------
# BLOCK 除錯
# --------------------
class OwlError():
    _dicts = {
                'YearError':'日期格式錯誤，請重新輸入。格式:yyyy',
                'SeasonError':'日期格式錯誤，請重新輸入。格式:yyyyqq',
                'MonthError':'日期格式錯誤，請重新輸入。格式:yyyymm',
                'DayError':'日期格式錯誤，請重新輸入。格式:yyyymmdd',
                'DateError':'資料日期輸入錯誤，請確認：起日期<迄日期',
                'YQMError':"欲查詢季資料，請輸入'Q'；年資料，請輸入'Y'；月資料，請輸入'M'",
                'SidError':'請重新確認您的股票代號是否輸入正確',
                'ColumnsError':'請重新確認您的欄位名稱是否輸入正確',
                'ValueError':'日期規則錯誤，請重新確認!!',
                'PdError':'無商品使用權限，請洽業務人員'
                }
    
    _http_error = {
        200 : '接受請求並回復結果',
        400 : '請求的網址參數不合法，請確認請求資料的正確性',
        401 : 'appid或appsecret輸入錯誤，請重新確認。',
        403 : '無商品使用權限，請洽業務人員',
        404 : '指定的請求不存在，請確認請求網址的正確性',
        500 : '內部系統錯誤，請稍後再試'
    }
    
    def __init__(self):
        '''
        錯誤狀況的提示訊息對應表

        [NOTES]
        ----------
            - 記錄各種操作錯誤情況
            - 包含 http 與 自訂錯誤
        '''
    @classmethod
    def table(cls):    
        return pd.Series(cls._dicts)
    
    @classmethod
    def http(cls):
        return pd.Series(cls._http_error)
