import os

path = 'C:\\Users\\julia\\Logs\\'
files = os.scandir(path)
data = []
results = []
for item in files:
    results.append([])
    if item.is_file():
        with open(path+item.name) as fp:
            lines = fp.readlines()
            data.append(lines)


for index, content in enumerate(data):
    while len(content) > 0:
        line = content.pop()
        if line in content:
            results[index].append(line)

for index, res in enumerate(results):
    with open(path + "result" + str(index) + ".txt", "w") as f:
        f.writelines(results[index])
print(results)

