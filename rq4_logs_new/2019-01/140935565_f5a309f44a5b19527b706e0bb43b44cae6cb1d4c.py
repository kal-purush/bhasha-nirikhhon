#! python3
# coding: utf-8

from sys import argv
import base


def delete():
    book_id = base.base_id
    try:
        book_id = int(argv[1])
    except Exception as e:
        print(argv[1], 'is not an integer.')
        return
    if book_id <= base.base_id:
        print(argv[1], 'is not an illegal id.')
        return
    status = base.read()
    if book_id - base.base_id > len(status):
        print('Book id', book_id, 'not exist.')
        return
    deleted_book = status.pop(book_id - base.base_id - 1)
    if input("Delete item:\n  %s %s/%s\nSure? (Y/else) " % (
        deleted_book['book_name'], deleted_book['current_page'], deleted_book['book_page'])
    ) == 'Y':
        base.write(status)
        print('Deleted.')
    else:
        print('Canceled.')


if __name__ == '__main__':
    delete()