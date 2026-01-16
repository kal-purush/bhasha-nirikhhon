#1

"""
Library
"""

class Book:
    """
    Класс Книга
    """
    book_count = 0
    def __init__(self, name, author, pages, ISBN, flag = 0, reserved = 0):
        print(f'Book {name} was added')
        self.name = name
        self.author = author
        self.pages = pages
        self.ISBN = ISBN
        self.flag = flag
        self.reserved = reserved
        Book.book_count += 1


moby_dick = Book('Moby Dick', 'German Melvill', '842', '978-5-04-118395-0')
pinocchio = Book('Pinocchio', 'Carlo Collodi', '72', '978-5-699-88892-4')

class User:
    """
    Класс Пользователь
    """
    def __init__(self, name):
        self.name = name
        print(f'User {name} was added')

    def get_book(self, book):
        if book.flag == 0 and book.reserved == 0:
            print(f'Book {book.name} was taken by {self.name}')
            book.flag = 1
            book.reserved = 1
        elif book.flag == 1:
            print(f'Not possible to take {book.name} by {self.name}')
        elif book.flag == 0 and book.reserved == 1:
            print(f'{self.name} trying to take reserved book {book.name}')
            book.reserved = 0

    def ret_book(self, book):
        if book.flag == 0:
            print(f'Book {book.name} already in library')
        elif book.flag == 1:
            print(f'{self.name} return book {book.name} in library')
            book.flag = 0
            book.reserved = 0

    def res_book(self, book):
        if book.flag == 0 and book.reserved == 0:
            print(f'{self.name} reserved book {book.name}')
            book.reserved = 1
        elif book.flag == 1:
            print(f'Not possible to reserved book {book.name}')
        elif book.reserved == 1:
            print(f'Book {book.name} is reserved by another person')


pavel = User('Pavel')
dima = User('Dima')
pavel.get_book(moby_dick)
dima.get_book(moby_dick)
pavel.ret_book(moby_dick)
dima.get_book(moby_dick)
pavel.res_book(pinocchio)
dima.res_book(pinocchio)
pavel.get_book(pinocchio)
pavel.get_book(pinocchio)


#2

class Investments:
    def __init__(self, amount, term, percent = 10):
        procent = 0
        for i in range(term):
            procent = (amount * percent) / 100
            amount = amount + procent
        return print(f'Deposit amount will be {amount}')


class Bank(Investments):    
    def __init__(self, name):
        self.name = name
        print(f'Client {name} was added')

    def deposit(self, amount, term):
        super().__init__(amount, term, percent = 10)
        pass

pavel = Bank('Pavel')
pavel.deposit(100, 7)


#3

class Students:
    def __init__(self, surname, initials, group, academic_performance):
        self.surname = surname
        self.initials = initials
        self.group = group
        self.academic_performance = academic_performance
        print(f'Student {surname} was added')

class School(Students):
    global good_students, automat_students, student_list
    good_students = {}
    automat_students = {} 
    student_list = {}
    def __init__(self, surname, initials, group, academic_performance):
        super().__init__(surname, initials, group, academic_performance)
        student_list[self.surname] = self.group
        print(f'Student {surname} in the school now') 

        a = academic_performance
        aver_score = sum(a) / len(a)

        if not 1 in a and not 2 in a and not 3 in a and not 4 in a and not 7 in a and not 8 in a and not 9 in a and not 0 in a:
            good_students[self.surname] = self.group

        if aver_score >= 7:
            automat_students[self.surname] = self.group


def get_good_student():
    return good_students

def get_automat_students():
    return automat_students

def student_from_group(group):
    for k, v in student_list.items():
        if v == group:
            print(k, v)
    

        


petrov = School('Petrov', 'I.I.', '1-a', [9, 6, 7, 9, 8])
sidorov = School('Sidorov', 'P.P', '1-a', [5, 5, 6, 5, 6])
ivanov = School('Ivanov', 'F.F.', '2-a', [6, 5, 6, 6, 5])

print(get_good_student())
print(get_automat_students())
print(student_from_group('2-a'))