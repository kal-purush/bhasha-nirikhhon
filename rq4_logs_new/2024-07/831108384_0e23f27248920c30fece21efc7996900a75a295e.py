import math

def floor(x):

    return int(math.floor(x))

def pmax(b,r):

    re = (math.sqrt((r/b)**2+8096)-(r/b))/b/2

    return floor(re)

def b2x(b):

    for r in range(1,b):
        pm = pmax(b,r)

        for p in range(1, pm+1):
            x = (p * b**2 + r)/b

            xf = floor(x)

            print(f"{x} * floor {xf} = {x*xf}")


def main():
    for b in range(1,45):
        b2x(b)

if __name__ == "__main__":
    main()