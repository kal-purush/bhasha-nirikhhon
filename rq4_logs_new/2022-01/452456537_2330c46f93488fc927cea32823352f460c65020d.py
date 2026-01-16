from collections import Counter

with open("input") as f:
    lines = f.readlines()
    count = 0
    for line in lines:
        policy, password = line.strip("\n").split(": ")
        count_range, letter = policy.split(" ")
        low = int(count_range.split("-")[0])
        high = int(count_range.split("-")[1])
        actual_count = dict(Counter(password)).get(letter)
        if actual_count != None and low <= actual_count <= high:
            count += 1
    print(count)
