"""
This problem was asked by Facebook.

Given a number in Roman numeral format, convert it to decimal.

The values of Roman numerals are as follows:

{
    'M': 1000,
    'D': 500,
    'C': 100,
    'L': 50,
    'X': 10,
    'V': 5,
    'I': 1
}
In addition, note that the Roman numeral system uses subtractive notation for numbers such as IV and XL.

For the input XIV, for instance, you should return 14.
"""

PAIRS = {
    'CM': 900,
    'CD': 400,
    'XC': 90,
    'XL': 40,
    'IX': 9,
    'IV': 4
}

SINGLES = {
    'M': 1000,
    'D': 500,
    'C': 100,
    'L': 50,
    'X': 10,
    'V': 5,
    'I': 1
}


def decimate(s, total=0):
    if not s:
        return total

    if s[:2] in PAIRS:
        total += PAIRS[s[:2]]
        return decimate(s[2:], total)
    else:
        total += SINGLES[s[:1]]
        return decimate(s[1:], total)


if __name__ == '__main__':
    print(decimate("XIV"))