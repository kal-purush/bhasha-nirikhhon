from typing import List

DIRECTIONS_TO_SEARCH = [(0, 1), (1, 1), (1, 0), (1, -1), (0, -1), (-1, -1), (-1, 0), (-1, 1)]


def get_wordsearch_rows() -> List[List[str]]:
    with open("day_four_input.txt") as input_file:
        return [list(row.strip()) for row in input_file.readlines()]


def get_number_of_words_found_from_position(wordsearch_rows: List[List[str]], row_index: int, column_index: int,
                                            num_of_rows: int, num_of_columns: int, word_to_find: str) -> int:
    if word_to_find[0] != wordsearch_rows[row_index][column_index]:
        return 0

    word_length = len(word_to_find)
    number_found_from_position = 0

    for direction in DIRECTIONS_TO_SEARCH:
        count = 1
        x, y = column_index, row_index
        while count < word_length:
            current_x = x + direction[1]
            current_y = y + direction[0]
            if current_y < 0 or current_y >= num_of_rows or current_x < 0 or current_x >= num_of_columns:
                break
            if wordsearch_rows[current_y][current_x] != word_to_find[count]:
                break

            count += 1
            x = current_x
            y = current_y

        if count == word_length:
            number_found_from_position += 1

    return number_found_from_position


if __name__ == "__main__":
    wordsearch_rows: List[List[str]] = get_wordsearch_rows()
    word_to_find: str = "XMAS"
    num_of_rows: int = len(wordsearch_rows)
    num_of_columns: int = len(wordsearch_rows[0])
    num_of_word_occurrences = 0
    for row_index in range(num_of_rows):
        for column_index in range(num_of_columns):
            num_of_word_occurrences += get_number_of_words_found_from_position(wordsearch_rows, row_index, column_index, num_of_rows, num_of_columns,
                                                       word_to_find)
    print(num_of_word_occurrences)