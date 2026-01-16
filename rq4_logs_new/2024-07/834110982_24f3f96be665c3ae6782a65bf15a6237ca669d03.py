import json


class Book:
    STATUS_AVAILABLE = "в наличии"
    STATUS_ISSUED = "выдана"

    def __init__(self, id, title, author, year, status):
        if status not in {self.STATUS_AVAILABLE, self.STATUS_ISSUED}:
            raise ValueError(f"Недопустимый статус: {status}")
        self.id = id
        self.title = title
        self.author = author
        self.year = year
        self.status = status

    def set_status(self, new_status):
        if new_status not in {self.STATUS_AVAILABLE, self.STATUS_ISSUED}:
            raise ValueError(f"Недопустимый статус: {new_status}")
        self.status = new_status


class Library:
    def __init__(self):
        self.books = []
        self.load_books()

    def load_books(self):  # Загрузка списка книг из файла library.json
        try:
            with open('library.json', 'r', encoding='utf-8') as file:
                data = json.load(file)
                for book_data in data:
                    book = Book(book_data['id'],
                                book_data['title'],
                                book_data['author'],
                                book_data['year'],
                                book_data['status'])
                    self.books.append(book)
        except FileNotFoundError:
            pass

    def save_books(self):  # Сохранение изменений в файле library.json
        with open('library.json', 'w', encoding='utf-8') as file:
            data = []
            for book in self.books:
                data.append({
                    'id': book.id,
                    'title': book.title,
                    'author': book.author,
                    'year': book.year,
                    'status': book.status
                })
            json.dump(data, file, ensure_ascii=False, indent=4)

    def add_book(self, title, author, year):  # Добавление книги
        if not self.books:
            new_id = 1
        else:
            new_id = max(book.id for book in self.books) + 1
        new_book = Book(new_id, title, author, year, Book.STATUS_AVAILABLE)
        self.books.append(new_book)
        self.save_books()
        print(f"Книга добавлена: {title} ({author}, {year}) с id {new_id}")

    def delete_book(self, book_id):  # Удаление книги
        for book in self.books:
            if book.id == book_id:
                self.books.remove(book)
                self.save_books()
                print(f"Книга с id {book_id} удалена")
                return
        print(f"Книга с id {book_id} не найдена")

    def search_books(self, query):  # Поиск
        found_books = []
        for book in self.books:
            if query.lower() in book.title.lower() or query.lower() in book.author.lower() or query == str(book.year):
                found_books.append(book)
        if found_books:
            print(f"Найдены книги по запросу '{query}':")
            for book in found_books:
                print(
                    f"ID: {book.id}, "
                    f"Название: {book.title}, "
                    f"Автор: {book.author}, "
                    f"Год: {book.year}, "
                    f"Статус: {book.status}")
        else:
            print(f"Книги по запросу '{query}' не найдены")

    def display_books(self):  # Показ списка книг
        if not self.books:
            print("Библиотека пуста")
        else:
            print("Список всех книг в библиотеке:")
            for book in self.books:
                print(
                    f"ID: {book.id}, "
                    f"Название: {book.title}, "
                    f"Автор: {book.author}, "
                    f"Год: {book.year}, "
                    f"Статус: {book.status}")

    def change_status(self, book_id, new_status):  # Изменение статуса книги
        try:
            book_id = int(book_id)
        except ValueError:
            print("Некорректный ID. Пожалуйста, введите числовой ID.")
            return

        for book in self.books:
            if book.id == book_id:
                try:
                    book.set_status(new_status)
                    self.save_books()
                    print(f"Статус книги с id {book_id} изменен на '{new_status}'")
                except ValueError as e:
                    print(e)
                return

        print(f"Книга с id {book_id} не найдена")

    def show_status(self, book_id):
        try:
            book_id = int(book_id)
        except ValueError:
            print("Некорректный ID. Пожалуйста, введите числовой ID.")
            return

        for book in self.books:
            if book.id == book_id:
                print(f"Статус книги '{book.title}' с ID {book_id}: {book.status}")
                return

        print(f"Книга с ID {book_id} не найдена")


def main():
    library = Library()
    while True:
        print("\nВыберите действие:")
        print("1. Добавить книгу")
        print("2. Удалить книгу")
        print("3. Найти книгу")
        print("4. Показать все книги")
        print("5. Изменить статус книги")
        print("6. Показать статус книги")
        print("7. Выйти из программы")

        choice = input("Введите номер действия: ")

        if choice == '1':
            title = input("Введите название книги: ")
            author = input("Введите автора книги: ")
            year = input("Введите год издания книги: ")
            library.add_book(title, author, year)
        elif choice == '2':
            book_id = input("Введите ID книги для удаления: ")
            try:
                book_id = int(book_id)
                library.delete_book(book_id)
            except ValueError:
                print("Некорректный ID. Пожалуйста, введите числовой ID.")
        elif choice == '3':
            query = input("Введите запрос (название, автор или год издания): ")
            library.search_books(query)
        elif choice == '4':
            library.display_books()
        elif choice == '5':
            book_id = input("Введите ID книги для изменения статуса: ")
            new_status = input("Введите новый статус (в наличии или выдана): ")
            library.change_status(book_id, new_status)
        elif choice == '6':
            book_id = input("Введите ID книги для показа статуса: ")
            library.show_status(book_id)
        elif choice == '7':
            print("Программа завершена")
            break
        else:
            print("Некорректный ввод. Пожалуйста, выберите действие из списка.")


if __name__ == "__main__":
    main()
