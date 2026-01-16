class Solution:
    def reverseBits(self, n: int) -> int:
        binary = convert_to_binary(n)
        number = convert_to_decimal(binary)
        return number
def convert_to_binary(n):
    binary = []
    while(n>=1):
        binary.append(n%2)
        n = n//2
    padding = max(32 - len(binary),0)
    for i in range(0,padding):
        binary.append(0)
    return binary
def convert_to_decimal(number):
    length = len(number)
    number = [(2**(length-i-1)*number[i]) for i in range(0,length)]
    return sum(number)