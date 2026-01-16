# -*- coding: utf-8 -*-

print "Willkommen bei der Eingabe des Tagesmenüs."

menu = {}

while True:
    dish = raw_input("Bitte gib die Hauptspeise ein: ")
    price = raw_input("Gib bitte den Preis in Euro ein: ")

    print "Die Speise " + dish + " Kostet " + price + "€"

    new = raw_input("Möchtest du eine weitere Speise erfassen? Ja/Nein: ")

    menu[dish] = price

    if new == "nein":
       break

print "Das Tagesmenü lautet heute %s" % menu

with open("menue.txt", "w+") as todo_file:
    print "Unser heutiges Tagesmenü:"