N=int(input())
#문제처럼 0부터가 아닌 1부터 시작하기 위해 맨 앞에 * 추가
switch=["*"]+list(map(int,input().split()))

for _ in range (int(input())):
    sex,num=map(int,input().split())
    if sex==1:   #남학생
        i=1
        ex=i*num   #숫자의 배열 위치를 바꾼다
        while 1:
            if ex > N:   #스위치 개수 넘어가면 break
                break
            # 0이면1로, 1이면 0으로 바꾸기
            if switch[ex]==0:
                switch[ex]=1
            else:
                switch[ex]=0
            i+=1
            ex=i*num
    else:   #여학생
        i=1
        while 1:
            # 스위치 범위 넘어가지 않게
            if num-i <= 0 or num+i >= len(switch):
                break
            # 좌우가 같지 않으면 빠져나오기
            if switch[num-i]!=switch[num+i]:
                break
            #좌우가 0이면 1로 상태 바꾸기
            if switch[num-i]==0:
                switch[num-i]=1
                switch[num+i]=1
            #좌우가 1이면 0으로 상태 바꾸기
            else:
                switch[num-i]=0
                switch[num+i]=0  
            i+=1
        #좌우가 같지 않으면 받은 숫자만 상태 바꾸기
        if switch[num]==0:
            switch[num]=1
        else:
            switch[num]=0

switch.remove("*")

for i in range(0, len(switch), 20):  # 스위치 상태를 20개씩 출력
    print(*switch[i:i+20])