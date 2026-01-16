d_m = {'01': 31, '02': 28, '03': 31, '04': 30, '05': 31, '06': 
           30, '07': 31, '08': 31, '09': 30, '10': 31, '11': 30, '12': 31}

def DMY(date, month, year):
    '''
    Функция проверяет дату и возвращает значение 
    True или False в зависимости от 
    того, может ли она существовать.
    '''
    if 1000 > year and year > 9999 and \
    month not in d_m.keys():
        return False
    if _leap(year) == True:
        d_m['02'] = 29
    if date > d_m[month]:
        return False
    return True
def _leap(year):
    '''
    Функция проверяет год на високосность, и если год
    високосный, возвращает True, иначе False
    '''
    return not year % 4 != 0 or year % 100 == 0 and year % 400 != 0