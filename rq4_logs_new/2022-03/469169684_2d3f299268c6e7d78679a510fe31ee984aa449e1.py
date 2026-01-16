
import datetime

class assdata:
    def __init__(self):
        self.name = 'maindata'
        self.date = datetime.date.today()
        self.curass = []

    def printall(self):
        reply = []
        for ass in self.curass:
            reply.append(str(ass))
        return reply

    def addass(self, cset, course, name, duedate, reminder = 1):
        year, month, day = duedate.split('-')
        datedue = datetime.date(int(year), int(month), int(day))
        dict = {
                'set': cset,
                'course': course,
                'name': name,
                'duedate': datedue,
                'reminder': reminder,
                'currdate': datetime.date.today()
        }
        if dict in self.curass:
            return
        else:
            self.curass.append(dict)

    #ToDo: Delete assignment
    def delass(self, cset, course, name, duedate, reminder=1):
        year, month, day = duedate.split('-')
        datedue = datetime.date(int(year), int(month), int(day))
        dict = {'set': cset,
                'course': course,
                'name': name,
                'duedate': datedue,
                'reminder': reminder,
                'currdate': datetime.date.today()
        }
        if dict in self.curass:
            self.curass.remove(dict)
        else:
            return

    def listcourse(self, course):
        reply = []
        for ass in self.curass:
            if ass.get('course') ==  course:
                reply.append(str(ass))
        return reply
    
    def listset(self, cset):
        reply = []
        for ass in self.curass:
            if ass.get('set') == cset:
                reply.append(str(ass))
        return reply

    def duetoday(self):
        reply = []
        date_today = datetime.datetime.today()
        for ass in self.curass:
            ass_date_str = ass.get('duedate')
            ass_date_obj = datetime.datetime.strptime(ass_date_str, '%Y-%m-%d')
            if ass_date_obj.date() == date_today.date():
                reply.append(ass)
        return reply

    def duetomorrow(self):
        reply = []
        tomdate = datetime.datetime.today()
        tomdate += datetime.timedelta(days=1)
        for ass in self.curass:
            ass_date_str = ass.get('duedate')
            ass_date_obj = datetime.datetime.strptime(ass_date_str, '%Y-%m-%d')
            if tomdate.date() == ass_date_obj.date():
                reply.append(ass)
        return reply
            





#test hws = assdata()
#test hws.addass('c', '1620','lab10','20220125')