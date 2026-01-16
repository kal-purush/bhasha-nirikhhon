

import csv

fichier= csv.reader(open("/home/acathignol/Lignee/2D/Pmut0/results.txt", "rb"), delimiter='|')

for ligne in fichier:
 print ligne[0], ligne[1]
  