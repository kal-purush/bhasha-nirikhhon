'''
OBJECTIVE:
Generate a calendar table with necessary columns of date dimensions. 
Output to a dataframe and then to a CSV file to use in an application. 
See readme.md in git repo for more info
https://github.com/guimatheus92
'''

import pandas as pd
from datetime import timedelta, date, datetime
from dateutil.relativedelta import relativedelta

# Determine the start time of this script
startTime = datetime.now()

def DimensionCalendar():
    # Set important user-defined variables
    start_dt = pd.to_datetime("01-01-2020").date()
    end_dt = pd.to_datetime("02-01-2020").date()

    # Start the process
    print('Dimension calendar table process started from ' + str(start_dt) + ' until ' + str(end_dt))

    # Create dataframe
    DimensionCalendar = pd.DataFrame()

    while start_dt <= end_dt:
        #print(start_dt)
        # Short date (10/10/2021, 11/10/2021, ...)
        DT_SHORTDATE = pd.to_datetime(start_dt).date()
        # Year as int (2021, 2022, ...)
        NR_YEAR = pd.to_datetime(start_dt).date().year
        # Month as int (1, 2, ...)
        NR_MONTH = pd.to_datetime(start_dt).date().month
        # Calendar day as int (1, 2, ...)
        NR_DAY = pd.to_datetime(start_dt).date().day
        # Month name (January, February, ...)
        NM_MONTH = pd.to_datetime(start_dt).month_name()
        # Day of week name (Monday, Tuesday, ...)
        NM_DAYOFWEEK = pd.to_datetime(start_dt).day_name()
        # Month of year in "mm/yyyy" format (01/2021, 02/2021, ...)
        NR_MONTHYEAR = str(NR_MONTH) + "/" + str(NR_YEAR)
        # Year month name in "mmmm, yyyy" format (January, 2020, February, 2020, ...)
        NM_MONTHYEAR = NM_MONTH + ', ' + str(NR_YEAR)
        # Long date name (January 1, 2020, January 2, 2020, ...)
        NM_LONGDATE = NM_MONTH + ' ' + str(NR_DAY) + ', ' + str(NR_YEAR)
        # Day of week number as int (Monday=1, Sunday=7, ...)
        NR_DAYOFWEEK = pd.to_datetime(start_dt).dayofweek + 1
        # Day of year number as int (363, 364, ...)
        NR_DAYOFYEAR = pd.to_datetime(start_dt).dayofyear
        # Semester number of year (1, 2)
        NR_SEMESTER = 1 if NR_MONTH <= 6 else 2
        # Semester name (S01, S02, ...)
        NM_SEMESTER = 'S0' + str(NR_SEMESTER)
        # Four months of year (1, 2, ...)
        NR_FOURMONTHS = 1 if NR_MONTH in (1, 4) else 2 if NR_MONTH in (5, 8) else 3
        # Quarter number of year (1, 2, ...)
        NR_QUARTER = ((NR_MONTH-1) // 3) + 1
        # Quarter name (Q01, Q02, ...)
        NM_QUARTER = 'Q0' + str(NR_QUARTER)
        # Semester number of year (1, 2, ...)
        NR_BIMESTER = 1 if NR_MONTH in (1, 2) else 2 if NR_MONTH in (3, 4) else 3 if NR_MONTH in (5, 6) else 4 if NR_MONTH in (7, 8) else 5 if NR_MONTH in (9, 10) else 6
        # Bimester name (Q01, Q02, ...)
        NM_BIMESTER = 'B0' + str(NR_BIMESTER)
        # Relative year from current year (-1, 0, ...)
        NR_RELATIVEYEAR = relativedelta((DT_SHORTDATE - timedelta(days=1)), DT_SHORTDATE).years
        # Relative month from current month (-1, 0, ...)
        NR_RELATIVEMONTH = ((DT_SHORTDATE - timedelta(days=1)).year - DT_SHORTDATE.year) * 12 + ((DT_SHORTDATE - timedelta(days=1)).month - DT_SHORTDATE.month)
        # Relative day from current day (-1, 0, ...)
        NR_RELATIVEDAY = (DT_SHORTDATE - (DT_SHORTDATE - timedelta(days=1)) ).days
        # Short date as int in "yyyymmdd" format (20210101, 20210102, ...)
        NR_SHORTDATE = str(NR_YEAR) + str(NR_MONTH) + str(NR_DAY)
        # YearMonth as int in "yyyymm" format (202001, 202002, ...)
        NR_YEARMONTH = str(NR_YEAR) + str(NR_MONTH)
        # YearQuarter as int in "yyyyqq" format (20201, 20202, ...)
        NR_YEARQUARTER = str(NR_YEAR)+str(NR_QUARTER)
        # Indicator Is Weekday (1=True, 0=False)
        IC_ISWEEKDAY = 'Yes' if NR_DAYOFWEEK in (1,2,3,4,5) else 'No'
        # Indicator Is Weekend (1=True, 0=False)
        IC_ISWEEKEND = 'Yes' if NR_DAYOFWEEK in (6,7) else 'No'
        # Indicator Is Holiday (Yes, No)
        IC_ISHOLIDAY = 'No'
        # Indicator Is Current Year (Yes, No)
        IC_ISCURRENTYEAR = 'Yes' if DT_SHORTDATE.year == date.today().year else 'No'
        # Indicator Is Current Month (Yes, No)
        IC_ISCURRENTMONTH = 'Yes' if DT_SHORTDATE.month == date.today().month else 'No'
        # Indicator Is Current Date (Yes, No)
        IC_ISCURRENTDATE = 'Yes' if DT_SHORTDATE == date.today() else 'No'
        # Indicator Is Last Year of Date (Yes, No)
        IC_LASTYEAROFDATE = 'Yes' if DT_SHORTDATE.year == (date.today() - relativedelta(years=1)).year else 'No'
        # Indicator Is Last Month of Date (Yes, No)
        IC_LASTMONTHOFDATE = 'Yes' if DT_SHORTDATE.month == (date.today() - relativedelta(months=1)) else 'No'
        # Indicator Is Last Day of Date -- Yesterday (Yes, No)
        IC_LASTDAYOFDATE = 'Yes' if DT_SHORTDATE == (date.today() - timedelta(days=1)) else 'No'
        # Lineage field about when the data was loaded
        LINDATE = date.today()
        # Lineage field about where the data was loaded
        LINSOURCE = 'Automatically Ingestion'

        # Ceate a dataframe from fields above
        df_DimensionCalendar = pd.DataFrame(([[DT_SHORTDATE, NR_YEAR, NR_MONTH, NR_DAY, NM_MONTH, NM_DAYOFWEEK, NR_MONTHYEAR, NM_MONTHYEAR, NM_LONGDATE, NR_DAYOFWEEK, NR_DAYOFYEAR, NR_SEMESTER, NM_SEMESTER, NR_FOURMONTHS, NR_QUARTER, NM_QUARTER, NR_BIMESTER, NM_BIMESTER, NR_RELATIVEYEAR, NR_RELATIVEMONTH, NR_RELATIVEDAY, NR_SHORTDATE, NR_YEARMONTH, NR_YEARQUARTER, IC_ISWEEKDAY, IC_ISWEEKEND, IC_ISHOLIDAY, IC_ISCURRENTYEAR, IC_ISCURRENTMONTH, IC_ISCURRENTDATE, IC_LASTYEAROFDATE, IC_LASTMONTHOFDATE, IC_LASTDAYOFDATE, LINDATE, LINSOURCE]]), 
        columns=['DT_SHORTDATE', 'NR_YEAR', 'NR_MONTH', 'NR_DAY', 'NM_MONTH', 'NM_DAYOFWEEK', 'NR_MONTHYEAR', 'NM_MONTHYEAR', 'NM_LONGDATE', 'NR_DAYOFWEEK', 'NR_DAYOFYEAR', 'NR_SEMESTER', 'NM_SEMESTER', 'NR_FOURMONTHS', 'NR_QUARTER', 'NM_QUARTER', 'NR_BIMESTER', 'NM_BIMESTER', 'NR_RELATIVEYEAR', 'NR_RELATIVEMONTH', 'NR_RELATIVEDAY', 'NR_SHORTDATE', 'NR_YEARMONTH', 'NR_YEARQUARTER', 'IC_ISWEEKDAY', 'IC_ISWEEKEND', 'IC_ISHOLIDAY', 'IC_ISCURRENTYEAR', 'IC_ISCURRENTMONTH', 'IC_ISCURRENTDATE', 'IC_LASTYEAROFDATE', 'IC_LASTMONTHOFDATE', 'IC_LASTDAYOFDATE', 'LINDATE', 'LINSOURCE'], index=None)

        # Append all dataframe in the loop                                
        DimensionCalendar = pd.concat([DimensionCalendar, df_DimensionCalendar], ignore_index=True)

        start_dt += timedelta(days=1)
    return DimensionCalendar

# Export the dataframe into CSV file in case we want to analyze the data
#DimensionCalendar.to_csv(r'DimensionCalendar.csv', index=False)

# Print how long the script took to run
print(datetime.now() - startTime)