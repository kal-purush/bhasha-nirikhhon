from operations import add, sub, multiply, divide

if __name__ == "__main__":
    a, b = 4, 2
    
    print(f"{a} + {b} = {add(a, b)}")
    print(f"{a} - {b} = {sub(a, b)}")
    print(f"{a} x {b} = {multiply(a, b)}")
    print(f"{a} / {b} = {divide(a, b)}")