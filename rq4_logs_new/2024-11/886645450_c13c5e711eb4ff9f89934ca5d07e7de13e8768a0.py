def read_matrix():
    """Зчитує матрицю з вводу та повертає її розміри і елементи."""
    n, m = map(int, input().split())  # Зчитуємо кількість рядків і стовпців
    matrix = [list(map(int, input().split())) for _ in range(n)]  # Зчитуємо рядки матриці
    return n, m, matrix

def add_matrices(matrix_a, matrix_b):
    """Додає дві матриці однакових розмірів."""
    n = len(matrix_a)
    m = len(matrix_a[0])
    result = [[matrix_a[i][j] + matrix_b[i][j] for j in range(m)] for i in range(n)]
    return result

# Зчитуємо першу матрицю
n1, m1, matrix_a = read_matrix()

# Зчитуємо другу матрицю
n2, m2, matrix_b = read_matrix()

# Перевіряємо, чи однакові розміри матриць
if n1 != n2 or m1 != m2:
    print("ERROR")
else:
    # Складаємо матриці
    result_matrix = add_matrices(matrix_a, matrix_b)
    # Виводимо результат
    for row in result_matrix:
        print(" ".join(map(str, row)))