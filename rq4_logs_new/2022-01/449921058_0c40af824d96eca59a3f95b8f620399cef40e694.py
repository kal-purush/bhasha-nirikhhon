"""
-------------------------------------------------------
Food class 
-------------------------------------------------------
Author:  Akaash Saini
ID:      210279650
Email:   sain9650@mylaurier.ca
Section: CP164-L2
__updated__ = "2022-01-15"
-------------------------------------------------------
"""
from Food_utilities import read_foods, food_table, food_search

file = open('food.txt', "rt")

foods = read_foods(file)

file.close()

result = food_search(foods, 11, 0, False)

food_table(result)