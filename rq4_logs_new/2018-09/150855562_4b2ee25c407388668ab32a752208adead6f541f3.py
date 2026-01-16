import csv
from ship import Ship
from sort import selection_sort_by_capacity
from sort import merge_sort_by_num_of_cases
from datetime import datetime


def read_data_from_file():
    data_array = []
    try:
        with open('ship_data.csv') as csvin:
            reader = csv.reader(csvin)
            for row in reader:
                new_ship = Ship(row[0], int(row[1]), int(row[2]))
                data_array.append(new_ship)
    except FileNotFoundError:
        print("File doesn't exist")
    return data_array


def work_time(start_time, finish_time):
    return finish_time - start_time


if __name__ == "__main__":
    ship_array = read_data_from_file()
    print("Ship array:\n" + ship_array.__str__())
    start = datetime.now().microsecond
    print("\nSorted by capacity:\n" + selection_sort_by_capacity(ship_array).__str__())
    finish = datetime.now().microsecond
    print("Work time: " + work_time(start, finish).__str__())
    print("Change: " + str(Ship.change_num) + "\nCompare: " + str(Ship.compare_num))
    Ship.drop_count()
    start = datetime.now().microsecond
    print("\nSorted by cases:\n" +
          merge_sort_by_num_of_cases(ship_array).__str__())
    finish = datetime.now().microsecond
    print("Work time: " + work_time(start, finish).__str__())
    print("Change: " + str(Ship.change_num) + "\nCompare: " + str(Ship.compare_num))