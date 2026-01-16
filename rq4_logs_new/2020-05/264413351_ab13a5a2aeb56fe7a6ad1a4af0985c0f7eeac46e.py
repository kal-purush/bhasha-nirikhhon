'''This program gives the derivative of "x" raised to the nth power,
where n ranges from "-infinity to +infinity"'''
def derivative_x_pow(dif, num):
    '''The input should be integers, the function will give return the
    nth derivative of "x" raised to the power of the given number'''
    if not isinstance(num, int) or isinstance(num, bool):
        raise TypeError("unsupported format")
    if not isinstance(dif, int) or isinstance(dif, bool):
        raise TypeError("unsupported format")
    if dif <= 0:
        raise ValueError("invalid entry, enter a valid number to differentiate")
    if ((dif > num) and (num == 0)) or ((dif > num) and (num == 1)):
        return 0
    if dif >= num > 0:
        return 1
    fore = 1
    index = num
    for _ in range(1, dif+1):
        fore = fore * index
        index = index-1
    return f'{fore}(x^{index})'