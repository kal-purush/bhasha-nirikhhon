import numpy as np

from collections import deque


def find_dst(l, src):
    # find max index of max x such that x < src 
    # or argmax of l
    x = src-1
    while x > 0:
        if x in l:
            return l.index(x)
        x -= 1
    return np.argmax(l)


def move(l: deque):
    src = l.popleft()
    pickup = [l.popleft() for _ in range(3)]
    # print("pickup", pickup)
    i = find_dst(l, src)
    # print("dst", l[i])
    while pickup:
        l.insert(i+1, pickup.pop())
    l.append(src)
    return l
    

def parse(line):
    return deque([int(s) for s in line])


def main():
    #l = parse("389125467")
    l = parse("219347865")

    for _ in range(100):
        l = move(l)
        print(l)

    # i = l.index(1)
    # l.rotate(i)
    # print(l)
    ans = "".join(map(str, l))
    print(ans)


if __name__ == "__main__":
    main()