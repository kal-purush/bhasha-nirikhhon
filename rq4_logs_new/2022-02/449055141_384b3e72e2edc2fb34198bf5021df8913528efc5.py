def contains_duplicate(numbers):
    numbers.sort()
    previus_number = numbers[0]
    for number in numbers[1:]:
        if number == previus_number:
            return True
        previus_number = number
    return False

# pior caso O(n*log(n) + n)
# melhor caso O(n*log(n))
# caso medio O(n*log(n) + n/2)