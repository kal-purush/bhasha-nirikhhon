numbers=[]
for a in range(0, 10):
    number=int(input('enter your number: '))
    numbers.append(number)
for i in range(len(numbers)):
    for b in range (i+1,len(numbers)):
        if numbers[i]>numbers[b]:
            numbers[i],numbers[b]=numbers[b],numbers[i]
            print(numbers)
        