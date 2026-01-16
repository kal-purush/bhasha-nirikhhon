
def find_nth_prime_number(num):

    if num == 1:
        return 2
    i = 3
    count = 1
    current_prime = 0
    while count < num:

        if is_prime(i):
            count += 1
            #print(i)
            current_prime = i
        i+=2

    return current_prime


def is_prime(num):
    if num < 2:
        return False
    elif num == 2:
        return True
    elif is_even(num):
        return False

    i = 3
    while i <= num/3:
        if num % i == 0:
            return False
        i += 2
    return True

def is_even(num):
    return True if num % 2 == 0 else False


print(is_prime(3))
print(find_nth_prime_number(10001))