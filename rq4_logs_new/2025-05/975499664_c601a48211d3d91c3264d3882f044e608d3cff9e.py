def compute_hcf(x, y):
    # This function uses the Euclidean algorithm to find HCF
    while y:
        x, y = y, x % y
    return x

# Example usage
num1 = int(input("Enter first number: "))
num2 = int(input("Enter second number: "))

hcf = compute_hcf(num1, num2)
print(f"The HCF of {num1} and {num2} is {hcf}")