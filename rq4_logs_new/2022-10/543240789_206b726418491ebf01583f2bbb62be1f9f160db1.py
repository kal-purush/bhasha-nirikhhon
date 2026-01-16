from itertools import combinations
def generate(n):
    l=list(combinations("()",n))
    for i in l:
        nl = 0
        nr = 0
        for j in i:
            if j == '(':
                nr+=1
            elif j == ')':
                nl+=1
        if nl == nr:
            continue
        else:
            l.remove(i)
    return l

if __name__ == '__main__':
    n=1
    l=generate(1)
    print(l[0])