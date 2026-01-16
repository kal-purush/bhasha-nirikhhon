def collatz_basic_solution(input_data):
    max_length = 1
    max_number = 1
    for number in range(1, input_data +1):
        temp_number = number
        result = 1
        while number != 1:
            result += 1
            if number % 2:
                number = number * 3 + 1
            else:
                number = number // 2
        if max_length < result:
            max_length = result
            max_number = temp_number
    return max_length, max_number


import time
def main():
    started = time.time()
    print("Basic algorithm: ")
    print(collatz_basic_solution(1000000))
    elapsed = (time.time() - started)
    print(elapsed)
    

if __name__ == "__main__":
    main()