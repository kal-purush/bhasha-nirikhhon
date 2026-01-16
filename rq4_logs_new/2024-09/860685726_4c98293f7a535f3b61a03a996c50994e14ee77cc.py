# gcd_janedoe.py


def gcd(a: int, b: int) -> int:
    while b != 0:
        a, b = b, a % b
    return a


# Test cases
print(gcd(54, 24))  # Expected output: 6
print(gcd(48, 18))  # Expected output: 6
print(gcd(101, 10))  # Expected output: 1