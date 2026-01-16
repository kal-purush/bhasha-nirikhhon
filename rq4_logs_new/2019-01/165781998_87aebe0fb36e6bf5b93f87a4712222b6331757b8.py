items = [1,2,2,2,3,2,1,1,2,3,1,1,2,3,2,1,3,2,1,2,3,2,3,2,3,2,3,2,3,2,3,2,3,2]

def bucketsort(dataset):
    print(type(dataset))
    bucket = {'one':0,'two':0,'three':0}
    for i in range(len(dataset)):
        print(dataset[i])
        if dataset[i] == 1:
            bucket['one'] += 1
        elif dataset[i] == 2:
            bucket['two'] += 1
        elif dataset[i] == 3:
            bucket['three'] += 1
    return bucket


print(bucketsort(items))