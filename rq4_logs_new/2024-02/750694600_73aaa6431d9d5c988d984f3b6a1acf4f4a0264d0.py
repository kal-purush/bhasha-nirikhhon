start = int(input())
N=start
Count = []
cnt = 0
if N == 1:
    cnt = 0
    Count.append(cnt)
else :
    while N!=1 and N!=0:
        if N%3==0:
            N=N//3
            cnt +=1
        else :
            while N != 1 and N != 0:
                if N%2==0:
                    N = N//2
                    cnt +=1
                    continue
                else :
                    N -= 1
                    cnt +=1
    Count.append(cnt)

    cnt=1
    N=start-1
    while N!=1 and N!=0:
        if N%3==0:
            N = N // 3
            cnt += 1
        else:
            while N != 1 and N != 0:
                if N % 2 == 0:
                    N = N // 2
                    cnt += 1
                    continue
                else:
                    N -= 1
                    cnt += 1
    Count.append(cnt)

    cnt=2
    N=start-2
    while N!=1 and N!=0:
        if N%3==0:
            N = N // 3
            cnt += 1
        else:
            while N != 1 and N != 0:
                if N % 2 == 0:
                    N = N // 2
                    cnt += 1
                    continue
                else:
                    N -= 1
                    cnt += 1
    Count.append(cnt)

print(min(Count))