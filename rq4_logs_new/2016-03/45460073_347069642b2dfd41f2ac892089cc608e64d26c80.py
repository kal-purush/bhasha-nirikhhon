import codecs
import random

MIN_INGRED = 2
MAX_INGRED = 8

with open("vegetarian") as ingredients:
        content = ingredients.read().splitlines()

roulette_result = []
roulette_tries = random.randint(2,8)
already_selected = []
for i in range(0, roulette_tries):
    select = random.randint(0, len(content) -1)
    while select in already_selected:
        select = random.randint(0, len(content) -1)
    already_selected.append(select)
    roulette_result.append(content[select])

for string in roulette_result:
    print string.decode('utf-8', 'ignore')