sample = [[1,2,3,4],[5,6,7,8],[9,10,11,12],[13,14,15,16]]
start = True

def rotate(direction, times):
    rotated = []
    if((direction == 'right' and times % 4 == 1) or (direction == 'left' and times % 4 == 3)):
        for i in range(len(sample)):
            row = []
            for j in range(3,-1,-1):
                row.append(sample[j][i])
            rotated.append(row)
    elif((direction == 'right' and times % 4 == 2) or (direction == 'left' and times % 4 == 2)):
        for i in range(3,-1,-1):
            row = []
            for j in range(3,-1,-1):
                row.append(sample[i][j])
            rotated.append(row)
    elif((direction == 'right' and times % 4 == 3) or (direction == 'left' and times % 4 == 1)):
        for i in range(3,-1,-1):
            row = []
            for j in range(len(sample)):
                row.append(sample[j][i])
            rotated.append(row)
    else:
        rotated = sample

    for i in rotated:
        print(' '.join(map(str, i)))
    return rotated

while(start):
    directionInput = input('Rotate to (right/left): ')
    timesInput = int(input('How many times: '))
    sample = rotate(directionInput, timesInput)
    stop = input('Continue (y/n): ')
    if (stop == 'n'): start = False