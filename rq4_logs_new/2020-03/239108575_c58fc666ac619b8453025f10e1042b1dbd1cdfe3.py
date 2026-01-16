from typing import Tuple, List, Dict

PLAYERS = ['X', 'O']
FREE_CELL = ' '


def define_field() -> Tuple[List[int], List[str]]:
    """ Defines field size """
    option = ''
    options = ['3', '4', '5']
    columns = ['1', '2', '3', '4', '5']
    rows = ['a', 'b', 'c', 'd', 'e']

    print('Задайте размер поля:')

    while option not in options:
        print('Выберите: {}, {} или {}'.format(*options))
        try:
            option = str(input())
        except ValueError:
            pass

    for o in range(len(columns) - int(option)):
        columns.pop()
        rows.pop()

    return columns, rows


def init_field(columns: List[str], rows: List[str]) -> List[Dict[Tuple[str, str], str]]:
    """ Initialize field from sizes """
    field = []

    for row in rows:
        field.append(dict.fromkeys(zip(row * len(columns), columns), FREE_CELL))

    draw_field(field,columns,rows)

    return field


def draw_field(field: List[Dict[Tuple[str, str], str]], columns: List[str], rows: List[str]):
    """ Draws current field """
    print(' ', ' '.join(str(c) for c in columns))

    for cell, row in zip(field, rows):
        print(row, '|'.join(cell.values()))
    return


def player_move(player: str, field: List[Dict[Tuple[str, str], str]]):
    """ Gets players move and updates field """
    move = ('0', '0')

    cells = dict()
    for line in field:
        cells.update(line)

    print(f'Ваш ход, игрок "{player}" (укажите координаты ячейки формата "e5"):')

    while move not in cells.keys() or cells[move] in PLAYERS:
        try:
            row, col = list(str(input()))
            move = tuple(list(row + col))
            if cells[move] in PLAYERS:
                print('Эта ячейка уже занята - сделайте другой ход.')
        except ValueError:
            print('Неправильный формат координат ячейки')
            continue
        except KeyError:
            print('Такой ячейки нет на поле')
            continue

    for line in field:
        if move in line.keys():
            line[move] = player
    return


def check_state(field: List[Dict[Tuple[str, str], str]], columns: List[str], rows: List[str]) -> str:
    """ Checks if someone has won """

    state = ''
    lines_to_win = []
    columns_to_win = []
    diagonals_to_win = [[], []]
    diagonals = [[], []]

    for i in range(len(columns)):
        columns_to_win.append([])

        diagonals[0].append((rows[i], columns[i]))
        diagonals[1].append((rows[i], columns[::-1][i]))

    for line in field:
        lines_to_win.append(list(line.values()))    # gorizontals
        i = 0

        for pair in line:                           # verticals
            columns_to_win[i].append(line[pair])
            i += 1

        i = 0
        for diagonal in diagonals:                  # diagonals
            for pair in diagonal:
                try:
                    diagonals_to_win[i].append(line[pair])
                except KeyError:
                    pass
            i += 1

        lines_to_win = lines_to_win + diagonals_to_win + columns_to_win

    for line in lines_to_win:
        count_chars = dict.fromkeys(line, 0)

        for value in line:
            count_chars[value] += 1

        for player in PLAYERS:
            if count_chars.get(player) == len(columns):
                state = f'Игрок "{player}" побеждает!'

    return state


def play_game(field: List[Dict[Tuple[str, str], str]], columns: List[str], rows: List[str]) -> str:
    """ Gameplay: makes moves till someone wins or no more free space """
    max_moves = len(columns) * len(rows)
    i = 0
    while True:
        for player in PLAYERS:
            player_move(player, field)
            i += 1

            draw_field(field, columns, rows)
            check_state(field, columns, rows)

            state = check_state(field, columns, rows)

            if state != '':
                break
            elif i == max_moves:
                state = 'Ничья!'
                break
        if state != '':
            break
    return state


if __name__ == '__main__':
    columns, rows = define_field()
    field = init_field(columns, rows)
    state = play_game(field, columns, rows)
    print(state)