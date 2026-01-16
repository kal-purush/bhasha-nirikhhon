

filename = r".\bookmarks\Bookmarks from google.md"

with open(filename, encoding='utf-8') as f:
    content = f.read().splitlines()

import re
result = []
for x in content:
    matches = re.search("\[(.*?)\]\((.*?)\) *(.*)", x)
    if matches is not None:
        result.append([matches.group(1), matches.group(2), matches.group(3)])
        print(f"{matches.group(1)}, {matches.group(2)}, '{matches.group(3)}'")
        file1 = open(f'./output/{matches.group(1)}.md', 'x')
        file1.write(f'[{matches.group(2)}]({matches.group(2)}) {matches.group(3)}')
        file1.close()




