from classes import Node, Restaurant
import csv


def load_restuarant_data(file: str) -> list[Restaurant]:
    """Reads a given restaurants csv file and outputs a list of restaurants"""
    with open(file) as restaurants_file:
        reader = csv.reader(restaurants_file)
        list_of_restuarants = []

        next(reader)

        i = 0
        for row in reader:
            restuarant_node = Restaurant(
                identifier=i,
                coordinates=(int(row[2]), int(row[3])),
                name=row[0],
                price=int(row[4]),
                r_type=row[5],
                address=row[1]
            )
            list_of_restuarants.append(restuarant_node)