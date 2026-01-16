# -*- coding:Utf-8 -*-
# !/usr/bin/env python3.5
# ------------------------------------------------------------------------------
# Author:  Arthur JOVART
# E-mail:  jovart.arthur@gmail.com
# Name:    act4
# Purpose: Guess numbers in a list
# Created: 26/11/2015
# Python:  3.5
# Modules: random
# Licence: WTFPL (naméoh et oui elle existe)
# Notes : http://stackoverflow.com/a/9225336/3738545
# -------------------------------------------------------------------------------
import random

list = [random.randrange(1,10), random.randrange(1,10)]
i = 1
print(list)
while int(input("Entrez un nombre >")) not in list:
    i += 1
    print("Non, c'est pas ca")

print("Bravo!! Tu as trouvé en " + str(i) + " essais.")