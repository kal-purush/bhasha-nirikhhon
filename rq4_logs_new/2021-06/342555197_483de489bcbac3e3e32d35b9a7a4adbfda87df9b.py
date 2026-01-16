if __name__ == "__main__":
    B = []
    N = int(input())
    for i in range(N):
        a = input()
        A = a.split()
        A = [int(x) for x in A]
        B.append(A)
    
    s = []
    max = []
    index = []
    A_sum = 0
    tmp = 0
    for i in range(N):
        all = B[i][0] + B[i][1]
        A_sum += B[i][0]
        if tmp <= all:
            max.append(all)
            index.append(i)

    print(B)