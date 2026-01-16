from tspFuns import mergeSort, addPathToTour, calDistance, combineSubtours, countAvg, rebuildTour


def connectNeighborCities(cities):
    n = len(cities)
    tour = []
    dstSelected = []
    route = 0
    visited = [0 for i in range(n)]
    citiesY = mergeSort(cities, 2)
    citiesX = mergeSort(cities, 1)


    maxDiff=0
    for i in range(n-1):
        temp=citiesX[i+1][1]-citiesX[i][1]
        if temp>maxDiff:
            maxDiff=temp
    tmp=rangeX=maxDiff*2
    t=0
    while t<2:
        print "rangeX",rangeX
        for i in range(n):
            for j in range(i + 1,min(i +int(n/10),n)):
                if abs(citiesY[j][1] - citiesY[i][1]) <= rangeX:
                    visited, tour, dstSelected, route = addPathToTour(citiesY[i], citiesY[j], visited, tour,
                                                                      dstSelected, route)
        t=t+1
        rangeX=rangeX+tmp


    k = 0
    while k < len(visited):
        if visited[k] == 0:
            tour = tour + [[k]]
        k = k + 1
    print "number of sub-tours",len(tour)
    return tour, dstSelected, route, visited, rangeX


def connectSubtours(cities, avg, tour, visited, dstSelected, route):
    for i in range(len(tour)):
        for j in range(i + 1, len(tour)):
            cityA1 = tour[i][0]
            cityA2 = tour[i][len(tour[i]) - 1]
            cityB1 = tour[j][0]
            cityB2 = tour[j][len(tour[j]) - 1]
            d11 = calDistance(cities[cityA1], cities[cityB1])
            d12 = calDistance(cities[cityA1], cities[cityB2])
            d21 = calDistance(cities[cityA2], cities[cityB1])
            d22 = calDistance(cities[cityA2], cities[cityB2])
            if d11 <= d12 and d11 <= d21 and d11 <= d22 and d11<=avg:
                visited, tour, dstSelected, route = addPathToTour(cities[cityA1], cities[cityB1], visited, tour,
                                                                  dstSelected, route)
            if d12 <= d11 and d12 <= d21 and d12 <= d22 and d12<=avg:
                visited, tour, dstSelected, route = addPathToTour(cities[cityA1], cities[cityB2], visited, tour,
                                                                  dstSelected, route)
            if d21 <= d11 and d21 <= d12 and d21 <= d22 and d21<=avg:
                visited, tour, dstSelected, route = addPathToTour(cities[cityA2], cities[cityB1], visited, tour,
                                                                  dstSelected, route)
            if d22 <= d11 and d22 <= d12 and d22 <= d21 and d22<=avg:
                visited, tour, dstSelected, route = addPathToTour(cities[cityA2], cities[cityB2], visited, tour,
                                                                  dstSelected, route)


    return tour, visited, dstSelected, route


def doTourInitialization(cities):
    n = len(cities)
    tour, dstSelected, route, visited, lim_y = connectNeighborCities(cities)
    route, tour, visited, dstSelected = combineSubtours(route, tour, visited, dstSelected)
    avg=countAvg(dstSelected,2)
    tmp=int(avg/2)

    while len(tour) > 1:
        tour, visited, dstSelected, route = connectSubtours(cities, avg, tour, visited, dstSelected, route)
        route, tour, visited, dstSelected = combineSubtours(route, tour, visited, dstSelected)
        avg=avg+tmp
        if len(tour[0])==n :
            break

    tour=tour[0]
    d = calDistance(cities[tour[0]], cities[tour[n - 1]])
    route = route + d
    c1=tour[0]
    c2= tour[n - 1]
    if c1>c2:
        c1,c2=c2,c1
    dstSelected.append([c1,c2, d])
    tour = rebuildTour(dstSelected, tour)
    return dstSelected, route, tour

