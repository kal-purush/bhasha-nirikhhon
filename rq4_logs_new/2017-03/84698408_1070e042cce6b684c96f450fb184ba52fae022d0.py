from enum import Enum
#
# Denver to El Paso (4)
# Kansas City to Houston (5)
# New York to Atlanta (6)
# Chicago to New Orleans (7), Calgary to Salt Lake City (7)
# Helena to Los Angeles (8), Duluth to Houston (8), Sault Ste Marie to Nashville (8)
# Montreal to Atlanta (9), Sault Ste. Marie to Oklahoma City (9), Seattle to Los Angeles (9), Chicago to Santa Fe (9)
# Duluth to El Paso (10), Toronto to Miami (10)
# Portland to Phoenix(11), Dallas to New York City (11), Denver to Pittsburgh (11), Winnipeg to Little Rock (11)
# Winnipeg to Houston (12), Boston to Miami (12)
# Vancouver to Santa Fe (13), Calgary to Phoenix(13), Montreal to New Orleans (13)
# Los Angeles to Chicago (16)
# San Francisco to Atlanta (17), Portland to Nashville (17)
# Vancouver to Montréal (20), Los Angeles to Miami (20)
# Los Angeles to New York City (21)
# Seattle to New York (22)

nodes = {0: "Vancouver", 1: "Calgary", 2: "Winnipeg", 3 : "Sault St. Marie", 4: "Montreal",
         5 : "Seattle", 6 : "Helena", 7: "Duluth", 8: "Toronto",9: "Boston",
         10: "Portland", 11: "Omaha", 12: "Chicago", 13: "Pittsburg", 14: "New York",
         15 : "San Francisco", 16: "Salt Lake City", 17: "Denver", 18: "Kansas City", 19: "Saint Louis",
         20: "Los Angeles", 21: "Las Vegas", 22: "Phoenix", 23: "Santa Fe", 24: "Oklahoma City",
         25: "Little Rock", 26: "Nashville", 27: "Raleigh", 28: "Washington",
         29: "El Paso", 30: "Dallas", 31: "Houston", 32: "New Orleans", 33: "Atlanta", 34: "Charleston",
         35: "Miami"}
class DestinationCards(Enum):
    # (node1, node2, points)
    D1 = (17, 29, 4)
    D2 = (18, 31, 5)
    D3 = (14, 33, 6)
    D4 = (12, 32, 7)
    D5 = (1, 16, 7)
    D6 = (6, 20, 8)
    D7 = (7, 31, 8)
    D8 = (3, 26, 8)
    D9 = (4, 33, 9)
    D10 = (3, 24, 9)
    D11 = (5, 20, 9)
    D12 = (12, 23, 9)
    D13 = (7, 29, 10)
    D14 = (8, 35, 10)
    D15 = (10, 22, 11)
    D16 = (14, 30, 11)
    D17 = (13, 17, 11)
    D18 = (2, 25, 11)
    D19 = (2, 31, 11)
    D20 = (9, 35, 12)
    D21 = (0, 23, 13)
    D22 = (1, 13, 13)
    D23 = (4, 32, 13)
    D24 = (12, 20, 16)
    D25 = (15, 33, 17)
    D26 = (10, 26, 17)
    D27 = (0, 4, 20)
    D28 = (20, 35, 20)
    D29 = (14, 20, 21)
    D30 = (5, 14, 22)

dests = [i.value for i in DestinationCards]