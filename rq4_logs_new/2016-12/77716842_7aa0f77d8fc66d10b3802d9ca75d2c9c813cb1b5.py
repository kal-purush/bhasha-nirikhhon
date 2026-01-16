from flask import Flask, render_template
from workalendar.europe import UnitedKingdom
from datetime import datetime, date

cal = UnitedKingdom()
today = date.today()

class recess_period(object):
    def __init__(self,name):
        self.name = name

recess_dates = {
    recess_period("Whitsun"):  ["26 May 2016", "6 June 2016"],
    recess_period("Recess"):   ["15 June 2016", "27 June 2016"],
    recess_period("Summer"):   ["21 July 2016", "5 September 2016"],
    recess_period("Conference"): ["15 September 2016", "10 October 2016"],
    recess_period("November"): ["8 November 2016", "14 November 2016"],
    recess_period("Christmas"):    ["20 December 2016", "9 January 2017"],
    recess_period("February"): ["9 February 2017", "20 February 2017"],
    recess_period("Easter"):   ["30 March 2017", "18 April 2017"],
    recess_period("Whitsun"):  ["25 May 2017", "5 June 2017"]
}

for recess_period, lst in recess_dates.items():
    recess_pair = []
    for i in lst:
        dt_object = datetime.strptime(i, '%d %B %Y')
        recess_pair.append(datetime.date(dt_object))
    recess_pair = sorted(recess_pair)
    recess_dates[recess_period] = recess_pair

def check_if_sitting(d):
    sit = {"ans": "Yes", "info": "Here is the Order of Business.", "url": "https://www.parliament.uk/business/publications/business-papers/commons/agenda-and-order-of-business"}
    if not cal.is_working_day(d):
        sit = {"ans": "No", "info":"Today is not a working day."}
    else:
        for recess_period, dt_pair in recess_dates.items():
            if dt_pair[0] < d < dt_pair[1]:
                sit = {"ans": "No", "info": "The House of Commons has adjourned for the " + recess_period.name + " recess."}
    return sit

sitting = check_if_sitting(date(2017,1,14))

app = Flask(__name__)

@app.route("/")
def main():
    return render_template('index.html', data=sitting)

if __name__ == "__main__":
    app.run()