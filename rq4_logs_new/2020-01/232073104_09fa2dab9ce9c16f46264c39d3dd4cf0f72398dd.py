'''
csvwriter.py

Saves the solution that is made with a certain algorithm in csv file.

Author 0505 + Wouter

'''

import csv


def csvWriter(station_file,file,trajecten):
    """Returns a csv file with all the tracks"""

    # all the available stations
    stations = {}
    with open(station_file, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            stations[row[0]] = [row[1], row[2]]
    

    with open(file, mode="w") as file:
        # open the file where the solution will be written in
        csv_writer = csv.writer(file)

        for traject in trajecten:
            # track number
            csv_writer.writerow(["Traject " + str(traject + 1)])

            # writes out connection
            for connectie in trajecten[traject].connections:
                csv_writer.writerow([connectie.origin, connectie.destination, stations[connectie.origin][0],stations[connectie.origin][1], stations[connectie.destination][0], stations[connectie.destination][1], str(connectie.time)])
            
            # the total time a track cost
            csv_writer.writerow(["Total time of " + str(trajecten[traject].time) + " minutes."])