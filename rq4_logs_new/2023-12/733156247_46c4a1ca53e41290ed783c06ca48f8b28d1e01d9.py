from datetime import datetime

def provide_todays_date(str_format=True):
    today_date = datetime.now()
    return f'{today_date.day}-{today_date.month}-{today_date.year} 00:00' if str_format else (today_date.year, today_date.month, today_date.day)