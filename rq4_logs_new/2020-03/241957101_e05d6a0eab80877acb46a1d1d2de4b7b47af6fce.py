# How to use:
# 1. Install Python 3.
# 2. Install BeautifulSoup using "pip install bs4"
# 3. Visit https://store.steampowered.com/account/licenses/ and save the page as "licenses.html"
#    in the same folder as this script.
# 4. Run this script using "python comp-finder.py"
# 5. Check "licenses.txt" once the script finishes.
#
# What does it do:
# This script will go through the list of licenses on your account and find all of the Complimentary items.
# It will ignore free to play releases (aka anything with Remove).
#
# Why does this exist:
# Complimentary licenses do not always count towards your library count or drop cards
# so this can be useful for finding games to repurchase.

from bs4 import BeautifulSoup

with open("licenses.html", "r", encoding="utf-8") as file:
	soup = BeautifulSoup(file, "html.parser")

licenses = []
table = soup.find('table', {'class' : 'account_table'})
game_rows = table.find_all('tr')
for game in game_rows:
	line_items = game.find_all('td')
	if line_items:
		if line_items[2].text.strip() == "Complimentary" and not "Remove\n\n" in line_items[1].text.strip():
			licenses.append(line_items[1].text.strip())

with open("licenses.txt", "w", encoding="utf-8") as file:
	for game_name in licenses:
		file.write(game_name + "\n")