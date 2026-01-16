def create_board():
    return [[" " for _ in range(3)] for _ in range(3)]


def print_board(board):
    print("  0 1 2")
    for i in range(3):
        print(f"{i}", end=" ")
        for j in range(3):
            print(board[i][j], end=" ")
        print()


def check_winner(board, player):
    for row in board:
        if all(cell == player for cell in row):
            return True

    for col in range(3):
        if all(board[row][col] == player for row in range(3)):
            return True

    if all(board[i][i] == player for i in range(3)):
        return True
    if all(board[i][2 - i] == player for i in range(3)):
        return True

    return False


def is_board_full(board):
    return all(cell != " " for row in board for cell in row)


def is_valid_move(board, row, col):
    if not (0 <= row <= 2 and 0 <= col <= 2):
        return False
    return board[row][col] == " "


def main():
    board = create_board()
    current_player = "X"

    print("Добро пожаловать в игру Крестики-нолики!")
    print("Чтобы сделать ход, введите номер строки и столбца (от 0 до 2)")

    while True:
        print_board(board)

        try:
            row = int(input(f"Игрок {current_player}, введите номер строки: "))
            col = int(input(f"Игрок {current_player}, введите номер столбца: "))
        except ValueError:
            print("Ошибка! Введите числа от 0 до 2")
            continue

        if not is_valid_move(board, row, col):
            print("Неверный ход! Клетка занята или координаты некорректны")
            continue

        board[row][col] = current_player

        if check_winner(board, current_player):
            print_board(board)
            print(f"Игрок {current_player} победил!")
            break

        if is_board_full(board):
            print_board(board)
            print("Ничья!")
            break

        current_player = "O" if current_player == "X" else "X"


if __name__ == "__main__":
    main()