def parking(hour,day):
    if not 32>day:
        raise TypeError("not correct")
    elif not "23:59">= hour:
        raise TypeError("not correct")
    elif not "59">= hour[3:]:
        raise TypeError("not correct")
    if '19:00'<=hour<'20:59':
        raise TypeError("not correct")
        print('both')
    elif (day%2==0 and "00:00"<=hour<'18:59') or (day%2==1 and "21:00"<=hour<'23:59'):
        print('right')
    else:
        print('left')
hour1= str(input("Введите время в формате час:минуты: "))
day1 = int (input("Введите день месяца: "))
parking(hour1,day1)



