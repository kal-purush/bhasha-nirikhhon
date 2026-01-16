# do pracy z plikami csv mozna uzyc modulu wbudowanego
import csv

with open('osoby') as plik:
    # tworzymy czytnik pliku csv
    czytnik = csv.reader(plik)

    for line in czytnik:
        # line jest juz od razu listą z elementami
        print(line)

dane = ['Bartek', 'Mojo', 33]

with open('osoby', 'a') as plik:
    # tworzymy zapisywacz pliku csv
    zapisywacz = csv.writer(plik)

    # zapisujemy liste - automatycznie jest tworzony string
    zapisywacz.writerow(dane)


