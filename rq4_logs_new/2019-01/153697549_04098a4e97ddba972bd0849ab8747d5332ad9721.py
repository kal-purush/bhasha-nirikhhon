s = 7
t = 11
a = 5
b = 15
apples = [-2, 2, 1]
oranges = [5, -6]


def countApplesAndOranges(s, t, a, b, apples, oranges):
    apples_distances = []
    oranges_distances = []
    count_apples = 0
    count_oranges = 0
    for each in apples:
        apples_distances.append(a + each)
    for each in oranges:
        oranges_distances.append(b + each)
    for distance in apples_distances:
        if distance >= s and distance <= t:
            count_apples += 1
    for distance in oranges_distances:
        if distance >= s and distance <= t:
            count_oranges += 1
    print(count_apples)
    print(count_oranges)


countApplesAndOranges(s, t, a, b, apples, oranges)