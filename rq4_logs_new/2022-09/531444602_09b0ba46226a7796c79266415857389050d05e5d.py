from calendar import month
import datetime
# dateutil and relativedelta are needed when we want to add/subtract months
import dateutil
from dateutil.relativedelta import relativedelta


# We see how the format of date works in Python - the %A - day of the week, %B Monthname, %d is for number of the day, %Y is for the year
def formatsindate():
    newdate1 = datetime.date(1947, 8, 15)
    message = "India got independence on {:%A %B %d %Y}."
    print(message.format(newdate1))


# Add number to a date, in below function, we add 100 days to a date
def datedeltawithanumber():
    newdate2 = datetime.date(2022, 9, 1)
    # Use datetime's timedelta method to add/subtract the delta from a date
    delta1 = datetime.timedelta(100)
    Adddate = newdate2 + delta1
    message = "New date after adding 100 days to 9 September 2022 is {:%A %B %d %Y}"
    print(message.format(Adddate))

# Below function Adds two months to a date

def takeInput(caption):
    temp = input(caption)
    if (temp):
        return temp
    else:
        print('input was incorrect, type again.')
        tempNew = takeInput(caption)
        return tempNew


def addmonthstodate():
    yearvalue = takeInput("Enter the year in yyyy format:\n")
    monthvalue = takeInput("Enter the month in m format:\n")
    dayvalue = takeInput("Enter the year in d format:\n")

    x = int(yearvalue)
    y = int(monthvalue)
    z = int(dayvalue)
    newdate3 = datetime.date(x, y, z)

    newdate4 = newdate3 + relativedelta(months=2)
    message = "The date two months from date your entered would be :{:%d %B %Y}"
    print(message.format(newdate4))
    return (newdate4)


# formatsindate()
# datedeltawithanumber()
addmonthstodate()