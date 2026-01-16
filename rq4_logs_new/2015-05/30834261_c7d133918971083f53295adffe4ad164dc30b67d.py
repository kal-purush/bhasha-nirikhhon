def largest_product(length, str_of_digits):
    """ returns the largest product of length lenght in series from a
    string of digits

    args:
      length: the length of the series to be calculated
      str_of_digits: the string of digits to calculate the product from
    """
    max = 0
    for nums in succesive_seqs(length, str_of_digits):
        product = reduce(lambda x,y: x*y, nums)
        if product > max:
          max = product
    return max


def succesive_seqs(length, str_of_digits):
    """Generator takes a string of digits and returns succesive
    lists of contiguous digits ofn length len

    Args:
      length: number of items to take from list

      str_of_digits: the string of digits to split up
    """
    digit_list = [int(x) for x in list(str_of_digits)]
    for index in range(len(str_of_digits) - length):
        yield digit_list[index:index+length]
