import calendar
from datetime import datetime

class MyCalendar(calendar.LocaleHTMLCalendar):
    def __init__(self, username, linked_date: dict):
        calendar.LocaleHTMLCalendar.__init__(self, firstweekday=6, locale='ja_jp')
        self.username = username
        self.linked_date = linked_date

    def formatmonth(self, theyear, themonth, withyear=True):
        v = []
        a = v.append

        a('<table class="table table-bordered table-sm" style="table-layout: fixed;">')

        a('\n')
        a(self.formatmonthname(theyear, themonth, withyear=withyear))
        a('\n')
        a(self.formatweekheader())
        a('\n')
        for week in self.monthdays2calendar(theyear, themonth):
            a(self.formatweek(week, theyear, themonth))
            a('\n')
        a('</table><br>')
        a('\n')
        return ''.join(v)

    def formatweek(self, theweek, theyear, themonth):
        s = ''.join(self.formatday(d, wd, theyear, themonth) for (d, wd) in theweek)
        return '<tr>%s</tr>' % s

    def formatday(self, day, weekday, theyear, themonth):
        if day == 0:
            return '<td style="background-color: #eeeeee">&nbsp;</td>'
        else:
            html = '<td class="text-center {highlight}"><a href="{url}" style="color:{text}">{day}</a></td>'
            text = 'blue'
            highlight = ''
            date = datetime(year=theyear, month=themonth, day=day)
            date_str = date.strftime('%Y%m%d')
            if date_str in self.linked_date:
                if self.linked_date[date_str]:
                    highlight = 'bg-success'
                    text = 'white'
                elif date < datetime.now():
                    highlight = 'bg-secondary'
                    text = 'white'
                else:
                    highlight = 'bg-warning'
            return html.format(
                url='/todo/{}/{}/{}/{}'.format(self.username, theyear, themonth, day),
                text=text,
                day=day,
                highlight=highlight)