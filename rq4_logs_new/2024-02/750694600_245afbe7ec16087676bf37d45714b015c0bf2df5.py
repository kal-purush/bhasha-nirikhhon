'''
백준 1244 스위치 켜고 끄기

문제 해석
입력 - 스위치 개수 | 스위치의 상태(켜져 있으면 1 아님 0) | 학생 수 | 학생성별(1남2여), 받은 스위치 수
출력 - 스위치의 마지막 상태

문제 풀이 절차
아이디어
스위치 저장 리스트
남자일 때 배수 바꾸기 
여자일 때 번호 중심으로 +_ 1 씩늘려 갈때 같은상태라면 바꾼다 아님 자신만 바꿈
'''
n = int(input())
swich = list(input().split())
human = int(input())

for i in range(human):
    s, cnt = list(map(int,input().split()))
    
    if s == 1:
        for j in range(cnt-1, n, cnt):
            if swich[j] == '1':
                swich[j] == '0'
            else:
                swich[j] == '1'
    
    else:
        pass
#-------------------
'''
여성 부분을 구현하는데서 막혔다. 
whlie문을 사용했었는데 멈추지 않아서 어떻게 해야할지 감이 안잡혀서 포기.
'''