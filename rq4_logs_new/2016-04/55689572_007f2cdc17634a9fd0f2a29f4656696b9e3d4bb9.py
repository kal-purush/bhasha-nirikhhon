#!/usr/bin/env python
# -*- coding: utf-8 -*-

odgovor = 'da'
seznam = []

while odgovor != 'ne':
    odgovor = raw_input("Ali želiš dodati kakšen izdelek na seznam? ")
    if odgovor == 'da':
        seznam.append(raw_input("Prosim napiši izdelek ki ga želiš dodati: "))
        print("Seznam: %s" % seznam )
    elif odgovor == 'ne':
        break
    else:
        print('Prosim odgovori z "da" ali "ne"!')

print'Veselo nakupuj!'