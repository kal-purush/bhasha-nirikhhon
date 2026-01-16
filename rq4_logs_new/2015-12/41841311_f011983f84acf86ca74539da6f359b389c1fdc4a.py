import datetime

class Single_Item_Stock(object):
    def __init__(self, produce_name):
        self.name = produce_name
        self.date = []
        self.count = []
        self.note = []
        
    def add_stock(self, purchase_date, item_count, additional_note):
        self.date.append(purchase_date)
        self.count.append(item_count)
        self.note.append(additional_note)

    def remove_stock(self, item_consumed):
        self.remove = item_consumed
        # assume the oldest produce is going to be consumed first
        self.count[0] = self.count[0] -  self.remove
        return self.count

    def total_count(self):
        # this adds up all the item_count.
        total_count = 0 
        for item_count in self.count:
            total_count += item_count
        return total_count
    
    def total_days(self):
        # this always counts from the earliest date a produce item was purchased if the item is purchased more than 
        today = datetime.date.today()
        oldest = min(self.date)
        year = int(str(oldest)[0:4])
        month = int(str(oldest)[4:6])
        day = int(str(oldest)[6:8]) 
        total_days = (datetime.date(year, month, day) - today).days 
        return total_days


    def __str__(self):
        return "{}, {}, {}, {}".format(self.name, self.date, self.count, self.note)
   
if __name__== '__main__':
    apple = Single_Item_Stock('apple')
    apple.add_stock(20151213, 3, 'fresh and cheap')
    apple.add_stock(20151214, 6, 'worms :-(')
    print apple.total_count()
    print apple.total_days()
    print apple.remove_stock(2)


    