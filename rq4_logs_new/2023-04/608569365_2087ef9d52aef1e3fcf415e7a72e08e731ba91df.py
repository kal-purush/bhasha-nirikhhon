# My frequently used functions

def intentry(rangemax=0) -> int:
    """
    Returns integer from users entry, or 0 if entry == 'exit'
    :param rangemax: maximal range number, 0 if unlimited
    :return: int
    """
    maximum = ""
    if rangemax > 0:
        maximum = f" between 0 and {rangemax}"
    while True:
        entry = input(f"Enter some whole number{maximum}: -> ")
        if entry.isdigit():
            if rangemax != 0 and int(entry) > rangemax:
                print("Number is out of range")
            else:
                return int(entry)
        else:
            print("You should enter only natural numbers")