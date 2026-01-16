import json
import csv
from datetime import date
from datetime import timedelta
import datetime
import time
import pandas as pd

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

_CSV_Directory_ = ''
_JSON_Directory_ = ''

testDay = date.fromisoformat('2020-01-22')
trainDay = date.fromisoformat('2020-05-03')
endDay = date.fromisoformat('2020-05-04')
dayLen = 108
dtrain = []
dtest = []

input_shape = [0, 0, 0, 0]

def loadIntersection(jsonFilename):
    jsonMetaData = []
    with open(_JSON_Directory_ + jsonFilename) as jsonFile:
        jsonMetaData = json.load(jsonFile)
    return jsonMetaData

def loadCounties(csvFilename):
    csvData = []
    with open(_CSV_Directory_ + csvFilename) as csvFile:
        csvDriver = csv.DictReader(csvFile)
        for row in csvDriver:
            csvData.append(row)
    return csvData

# Compare two date. This function implemented for specefic use, getting baseDay object and counter, then compare it to targetDay in our Data format(e.g. 05/18/20)
def dayComp(baseDay, dayCounter, targetDay):
    day1 = (baseDay + timedelta(days=dayCounter)).isoformat()
    day2 = datetime.datetime.strptime(targetDay, '%m/%d/%y').strftime('%Y-%m-%d')
    if (day1 == day2):
        return True
    return False

def fromIsotoDataFormat(day):
    return day.strftime('%m/%d/%y')

def binary_search(countiesData, target_fips, target_date):
    target = (target_fips, datetime.datetime.fromisoformat(target_date))

    l = 0
    r = len(countiesData)

    # Find first row of target county
    while (1):
        mid = (r - l) // 2
        fips = int(countiesData[l + mid]['county_fips'], 10)

        if (fips == target[0] and l + mid > 0 and int(countiesData[l + mid - 1]['county_fips'], 10) != target[0]):
            l = l + mid
            r = l + 1000
            break

        elif (fips >= target[0]):
            r = l + mid

        else:
            l = l + mid + 1

        if (r == l):
            return -1

    # Find target day
    while (1):
        mid = (r - l) // 2
        fips = int(countiesData[l + mid]['county_fips'], 10)
        dataDate = datetime.datetime.strptime(countiesData[l + mid]['date'], '%m/%d/%y')

        if (fips == target[0] and dataDate == target[1]):
            return l + mid

        elif (fips > target[0] or (fips == target[0] and dataDate > target[1])):
            r = l + mid

        else:
            l = l + mid + 1

        if (r == l):
            return -1

if __name__ == "__main__":
    gridIntersection = loadIntersection('map_intersection_1.json')
    countiesData = loadCounties('full-temporal-data.csv')

    # ################################################################ creating image array(CNN input)

    # # each row on imageArray include image data on day i
    # imageArray = []

    # start_time = time.time()
    # for i in range(dayLen):
    #     grid = []
    #     for x in range(len(gridIntersection)):
    #         gridRow = []
    #         for y in range(len(gridIntersection[x])):
    #             gridCell = 0
    #             for county in gridIntersection[x][y]:
    #                 countyConfirmed = 0
    #                 for row in countiesData:
    #                     if (county['fips'] == int(row['county_fips'], 10) and dayComp(testDay, i, row['date'])):
    #                         countyConfirmed = round(float(row['confirmed']) * county['percent'])
    #                         break
    #                 gridCell += countyConfirmed
    #             gridRow.append([gridCell])
    #         grid.append(gridRow)
    #     imageArray.append(grid)

    # for i in range(len(imageArray)):
    #     print("day " + str(i))
    #     for x in range(len(imageArray[i])):
    #         for y in range(len(imageArray[i][x])):
    #             print(imageArray[i][x][y], end='')
    #         print('')
    #     print('')

    # print('\t|Execution time: {0}'.format(time.time() - start_time))

    ################################################################ creating image array(CNN input) ### Binary Search

    # each row on imageArray include image data on day i
    imageArray = []

    start_time = time.time()
    for i in range(dayLen):
        grid = []
        for x in range(len(gridIntersection)):
            gridRow = []
            for y in range(len(gridIntersection[x])):
                gridCell = 0
                for county in gridIntersection[x][y]:
                    index = binary_search(countiesData, county['fips'], (testDay + timedelta(days=i)).isoformat())
                    if (index != -1):
                        gridCell += round(float(countiesData[index]['confirmed']) * county['percent'])                        
                gridRow.append([gridCell])
            grid.append(gridRow)
        imageArray.append(grid)

    for i in range(len(imageArray)):
        print("day " + str(i))
        for x in range(len(imageArray[i])):
            for y in range(len(imageArray[i][x])):
                print(imageArray[i][x][y], end='')
            print('')
        print('')

    print('\t|Execution time: {0}'.format(time.time() - start_time))

    ################################################################ split imageArray into train Data(dtrain) and test Data(dtest)

    input_shape = [dayLen - 14, len(gridIntersection), len(gridIntersection[0]), 1]
    dtrain = imageArray[:-14]
    dtest = imageArray[-14:]

    # Clear memory
    gridIntersection.clear()
    countiesData.clear()

    # ################################################################ init model
    model = keras.Sequential()
    # # Conv2D parameters: filters, kernel_size, activation, input_shape
    model.add(tf.keras.layers.Conv2D(16, (3, 3), activation='relu', input_shape=input_shape))
    model.add(tf.keras.layers.MaxPooling2D(2,2))
    # model.add(keras.input(shape=(len(imageArray), len(imageArray[0]), len(imageArray[0][0]), len(imageArray[0][0][0]))))
    # model.add(layers.Dense(len(imageArray) * len(imageArray[0]) * len(imageArray[0][0]) * len(imageArray[0][0][0]))))
    # model.add()