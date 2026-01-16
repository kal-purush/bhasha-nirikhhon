from helpers import get_sorted_rows, validate_category, validate_sat


def test_get_sorted_rows():
    rows = [
        {'text': 'и тут тоже неважно', 'time': '00:29:17'},
        {'text': 'все равно что тут', 'time': '00:16:20'},
        {'text': 'и тут все равно', 'time': '01:02:07'},
    ]
    waiting_rows = [
        {'text': 'все равно что тут', 'time': '00:16:20'},
        {'text': 'и тут тоже неважно', 'time': '00:29:17'},
        {'text': 'и тут все равно', 'time': '01:02:07'}
    ]
    result = get_sorted_rows(rows)
    assert waiting_rows == result


def test_validate_category():
    cat_incorrect = 'Ж10-17'
    cat_correct_1 = 'М10'
    cat_correct_2 = 'М10-14'
    assert not validate_category(cat_incorrect)
    assert validate_category(cat_correct_1)
    assert validate_category(cat_correct_2)


def test_validate_sat():
    sat_incorrect_1 = '12.99.2022'
    sat_incorrect_2 = '12/99'
    sat_correct = '16.09.2023'
    assert not validate_sat(sat_incorrect_1)
    assert not validate_sat(sat_incorrect_2)
    assert validate_sat(sat_correct)