'''What's the least/greatest number of Friday the 13ths in a year?
Peter Farrell with Curtis
Friday, October 13, 2017'''

class Month(object):
    global days, m_names,m_lengths
    days = ['Monday',
            'Tuesday',
            'Wednesday',
            'Thursday',
            'Friday',
            'Saturday',
            'Sunday']
    m_names = ['January','February','March','April','May','June',
               'July','August','September','October','November',
               'December']
    m_lengths = {'January':31,'February':28,'March':31,'April':30,
               'May':31,'June':30,
               'July':31,'August':31,'September':30,'October':31,
               'November':30,'December':31}
    
    def __init__(self,name,start_day):
        global days,m_names,m_lengths
        self.name = name
        self.length = m_lengths[self.name]
        self.start_day = start_day
        
        self.calendar = dict()
        #find index of start day
        index1 = days.index(self.start_day)
        self.next_month_start = days[(index1 + self.length) %7]
        for i in range(1,self.length+1):
            #keep track of day of week
            day_of_week = days[(index1 + i - 1) % 7]
            self.calendar[i] = day_of_week

    def report(self):
        '''Returns True if the 13th is a Friday'''
        if self.calendar[13] == 'Friday':
            return True
        else:
            return False

class Year(object):
    global m_names, m_lengths
    m_names = ['January','February','March','April','May','June',
               'July','August','September','October','November',
               'December']
    
    
    def __init__(self,name,start_day):
        global m_names,m_lengths
        self.friday_the_thirteenths = 0
        self.months = [] #for saving month objects
        self.name = name
        self.start_day = start_day
        for i,mname in enumerate(m_names):
            if mname == 'January':
                self.months.append(Month(mname,
                                  self.start_day))
            else:
                #look for previous month, calculate start day
                start = self.months[i-1].next_month_start
                self.months.append(Month(mname,start))
            #testing
            #print(self.months[i].name,self.months[i].calendar[13])
            if self.months[i].report():
                self.friday_the_thirteenths += 1
        print(self.name,self.friday_the_thirteenths)

for day in days:
    
    myyear = Year(day,day)
