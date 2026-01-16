'''
입력 : 스위치 개수, [스위치 상태], 학생수, (성별, 받은 수)

'''

N = int(input())
switches = list(map(int, input().split()))
students = int(input())
for student in range(students):
    gender, get = map(int, input().split())

    ## 학생이 여자라면
    if gender == 2:
        idx = get-1
        i = 1
        while 0 <= idx-i and idx+i<N:
            if switches[idx-i] == switches[idx+i]:
                i+=1
        else :
            for j in range(idx-i+1,idx+i):
                if switches[j] == 1:
                    switches[j] = 0
                else :
                    switches[j] = 1

    ## 학생이 남자라면
    elif gender == 1:
        n = 1
        while n*get<=N:
            if switches[n*get-1] :
                switches[n*get-1] = 0
                n+=1
            else :
                switches[n*get-1] = 1
                n+=1

for i in range(0, N, 20):
    print(*switches[i:i+20])