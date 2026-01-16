from f_google.sheets.sheet import Sheet


def test_col_index_to_letter():
    assert Sheet.col_index_to_letter(index=1) == 'A'
    assert Sheet.col_index_to_letter(index=27) == 'AA'


def test_to_a1_range():
    first_row = 5
    last_row = 9
    first_col = 3
    last_col = 6
    a1 = 'C5:F9'
    assert Sheet.to_a1_range(first_row, last_row, first_col, last_col) == a1