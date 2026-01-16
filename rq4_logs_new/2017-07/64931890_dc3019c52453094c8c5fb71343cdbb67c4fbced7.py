def in_words(number):
    number = int(number)    
    digits = [
        'zero',
        'one',
        'two',
        'three',
        'four',
        'five',
        'six',
        'seven',
        'eight',
        'nine'
    ]
    second_ten = [
        None,
        'ten',
        'eleven',
        'twelve',
        'thirteen',
        'fourteen',
        'fifteen',
        'sixteen',
        'seventeen',
        'eighteen',
        'nineteen'
    ]
    decades = [
        None,
        None,
        'twenty',
        'thirty',
        'forty',
        'fifty',
        'sixty',
        'seventy',
        'eighty',
        'ninety'        
    ]
    hundred = 'hundred'
    thousand = 'thousand'
    a = 'and'
    s = ' '
    h = '-'
    
    if number < 0 or number > 1000:
        return None
    if number == 1000:
        return digits[1] + s + thousand
    
    number = str(number).rjust(3, '0')
    result = ''
    hundreds = int(number[0])
    tens = int(number[1])
    units = int(number[2])
    if hundreds:
        result += digits[hundreds] + s + hundred
        if tens or units:
            result += s + a + s
    if tens:        
        if tens == 1:
            result += second_ten[units + 1]
        else:
            if units:
                result += decades[tens] + h + digits[units]
            else:
                result += decades[tens]
        return result
    if units:        
        result += digits[units]
    return result
    

    