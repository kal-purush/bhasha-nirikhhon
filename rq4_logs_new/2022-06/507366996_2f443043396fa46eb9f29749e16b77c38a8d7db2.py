values = []

for i in range(5):
    number = (int(input("Enter a number: ")))
    if len(values) == 0:
        values.append(number)
    else:
        for j in range(len(values)):
            if number < values[j]:
                values.insert(j, number)
                break
            elif j == len(values)-1:
                values.append(number)
                break

print(values)