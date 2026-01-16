class Data:
    def __init__(self, date=input('Введите сегоднешнюю дату в формате "день-месяц-год": ')):
        self.date = date



    def make_func(self, param):
        return list(map(int, param.split('-')))


    @classmethod
    def int_data(cls):
        return list(map(int, (Data().date).split('-')))


    def help_func(self):
        return Data.check_right(self.date)


    @staticmethod
    def check_right(var):
        if Data().make_func(var)[1] < 1 or Data().make_func(var)[1] > 12:
            return 'False'
        if Data().make_func(var)[0] < 1 or Data().make_func(var)[0] > 31:
            return 'False'
        if Data().make_func(var)[2] != 2020:
            return 'False'
        else:
            return 'Valid'


my_class = Data()
print(my_class.int_data())
print(my_class.help_func())