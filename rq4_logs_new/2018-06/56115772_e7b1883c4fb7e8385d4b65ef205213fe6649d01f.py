def n013_roman_to_integer(roman):
    ROMAN_VALUES = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000,
    }
    num = 0
    for ind, i in enumerate(roman):
        # at last one
        if len(roman) == ind + 1:
            num += ROMAN_VALUES[i]
        # if next val is bigger
        elif ROMAN_VALUES[roman[ind+1]] > ROMAN_VALUES[i]:
            num -= ROMAN_VALUES[i]
        else:
            num += ROMAN_VALUES[i]

    return num