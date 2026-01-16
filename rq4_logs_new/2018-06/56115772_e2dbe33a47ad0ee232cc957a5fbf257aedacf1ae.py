# Time:  O(1)
# Space: O(1)
#
# Determine whether an integer is a palindrome. Do this without extra space.
#
# Some hints:
# Could negative integers be palindromes? (ie, -1)
#
# If you are thinking of converting the integer to string, note the restriction of using extra space.
#
# You could also try reversing an integer. However, if you have solved the problem "Reverse Integer",
# you know that the reversed integer might overflow. How would you handle such case?
#
# There is a more generic way of solving this problem.
#


def n009_palindrome_number(integer):
    # ignore negative
    if integer < 0:
        integer = -integer
    for i in range((len(str(integer))+1)/2):
        if str(integer)[i] != str(integer)[-1-i]:
            return False
    return True