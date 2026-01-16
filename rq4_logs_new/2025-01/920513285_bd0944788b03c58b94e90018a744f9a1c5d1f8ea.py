def how_many_ways(num_steps:int) -> str:
    value1:int = 1
    value2:int = 1
    for i in range(num_steps - 1):
        temp:int = value1
        value1 = value1 + value2
        value2 = temp
        print(value1, value2)
    return f"Number of ways to reach {num_steps} is {value1}"

def main():
    num_steps:int = int(input("How many steps: "))
    print(how_many_ways(num_steps))

if __name__ == "__main__":
    main()