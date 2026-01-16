import matplotlib.pyplot as plt
import numpy as np
import networkx as nx


def row_valid(row, num_vertices=0):
    """
    Checks if the row is valid and converts it to int
    :param row: list
    :param num_vertices: int
    :return: list
    """
    # Перевірка на довжину рядка
    if num_vertices == 0 or len(row) == num_vertices:
        # Якщо рядок правильний і складається з 0 і 1, то повертаємо його
        if all(x in '01' for x in row):
            row = list(map(int, row))
            return row
    # Як ні, то помилка вводу
    raise ValueError("Input is wrong")


def read_input():
    """
    Reads an adjacent matrix row by row
    :return: list
    """
    print("Введіть матрицю суміжності порядково, розділяючи одиниці ти нулі "
          "ПРОБІЛАМИ (не комами), наприклад: 1 0 0 1")

    first_row = input().split()  # Зчитування першого рядка

    # перевірка на правильнісь вводу
    first_row = row_valid(first_row)

    # Записую перший рядок в матрицю та визначаю кількість вершин
    matrix = [first_row]
    num_vertices = len(first_row)

    # Проходжусь по решті рядків, зчитую їх в матрицю
    for i in range(1, num_vertices):
        row = input().split()

        # перевірка на правильнісь вводу
        row = row_valid(row, num_vertices)

        # Зчитую рядок в матрицю
        matrix.append(row)

    return matrix


def read_file():
    """
    Reads a matrix from matrix.txt
    :return: list
    """
    matrix = []
    # Зчитую дані з файлу в матрицю
    with open('matrix.txt', 'r', encoding='utf-8') as f:
        for row in f:
            # Перевірка вводу
            row = row_valid(row.strip().split())
            matrix.append(row)
    return matrix


def check_color(matrix, color, colored_vertices, vertex):
    """
    Returns True if we can use this color in vertex, False otherwise
    :param colored_vertices: list
    :param matrix: list
    :param color: string
    :param vertex: int
    :return: bool
    """
    # Проходжусь по вершинах, які вже мають колір
    for i in range(0, vertex):
        # Якщо вершини суміжні, та її колір співпадає з нашим, то не підходить
        if matrix[vertex][i] and colored_vertices[i] == color:
            return False
    # Інакше, ми можемо використати цей колір
    return True


def backtracking_search(matrix, vertex, colored_vertices, color_list):
    """
    Assigns one of 4 colors (Red, Green, Blue, Yellow) to every vertex,
    returns False if not possible, using Backtracking
    :param matrix: int
    :return: list
    """
    # Якщо ми дійшли до останньої вершини, то розв'язок знайдено
    if vertex == len(colored_vertices):
        return colored_vertices

    # Перебираємо всі можливі кольори:
    for color in color_list:
        # Якщо ми можемо вибрати даний колір, то беремо його і йдемо далі
        if check_color(matrix, color, colored_vertices, vertex):
            colored_vertices[vertex] = color
            result = backtracking_search(matrix, vertex+1,
                                         colored_vertices, color_list)

            # Якщо розв'язок існує, повертаємо його і йдемо назад
            if result:
                return result
    # Якщо всі варіанти перебрані і розв'язку не знайдено, повертаємо False
    return False


def graph_visualize(matrix, color_map):
    """
    Visualize the graph in a convenient way
    :param matrix: list
    :param color_map: list
    :return: None
    """
    # Перетворення матриці в NumPy array(потрібно для візуалізації)
    data = nx.from_numpy_matrix(np.array(matrix))
    # Власне малюнок графа
    nx.draw(data, node_color=color_map, with_labels=True)
    # Вивфд користувачу
    plt.show()


def main():
    """
    Main function that runs the program
    """
    # Читання матриці суміжності з файлу чи терміналу
    while True:
        input_method = input("If you want to read matrix from matrix.txt, "
                             "enter 1, if from terminal manually, enter 0: ")
        if input_method == '0':
            matrix = read_input()
            break
        elif input_method == '1':
            matrix = read_file()
            break

    # Перевірка на планарність за формулою ейлера
    graph = nx.from_numpy_matrix(np.array(matrix))
    # Якщо граф непланарний, то я сходу можу дати відповідь(тисячні секунди)
    if not nx.algorithms.planarity.check_planarity(graph)[0]:
        print("It is not possible to color the graph in 4 colors")
        return None

    # Якщо граф планарний, то робимо бектрекінг
    color_list = ['red', 'green', 'blue', 'yellow']
    num_vertices = len(matrix)
    result = backtracking_search(matrix, 0, [0]*num_vertices, color_list)

    # Візуалізація
    graph_visualize(matrix, result)


if __name__ == '__main__':
    main()