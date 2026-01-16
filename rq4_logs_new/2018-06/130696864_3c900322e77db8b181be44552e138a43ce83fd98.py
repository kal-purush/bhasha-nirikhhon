def perm(list, start):

    if start is len(list)-1:
        print(list)
        return

    for i in range(start, len(list)):
        list[start], list[i] = list[i], list[start]
        # from the next start
        perm(list, start+1)
        list[i], list[start] = list[start], list[i]

if __name__ == "__main__":
    a = [1,2,3]
    perm(a, 0)