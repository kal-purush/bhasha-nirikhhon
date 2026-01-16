def generate_sequence(n):
    result = []
    current_number = 1
    while len(result) < n:
        result.extend([current_number] * current_number)
        current_number += 1
    return result[:n]

def main():
    try:
        n = int(input("Введите количество элементов последовательности: "))
        if n <= 0:
            print("Введите положительное число.")
            return
        sequence = generate_sequence(n)
        print(" ".join(map(str, sequence)))
    except ValueError:
        print("Пожалуйста, введите корректное число.")

if __name__ == "__main__":
    main()