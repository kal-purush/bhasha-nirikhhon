"""Restaurant rating lister."""

# Your job is to write a program named ratings.py that:

# Reads the ratings in from the file
# Stores them in a dictionary, and then
# Spits out the ratings in alphabetical order by restaurant
# put your code here


def rate_restaurant(file_name):
    """takes file name, returns sorted dict with restaurant:rating pairs"""

    with open(file_name) as the_file:
        
        restaurant_ratings = {}

        for line in the_file:
            line = line.rstrip()
            pair = line.split(':')
            restaurant = pair[0]
            rating = pair[1]
            restaurant_ratings[restaurant] = rating

        print restaurant_ratings

rate_restaurant("scores.txt")