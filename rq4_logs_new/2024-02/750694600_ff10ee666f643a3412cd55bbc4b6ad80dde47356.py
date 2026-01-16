# 100 x 100 배열을 만듭니다.
matrix = [[0]*100 for i in range(100)] 
#4개의 사각형을 만드니 4번 반복
for x in range(4):
    #왼쪽 아래 x 좌표, 왼쪽 아래 y 좌표, 오른쪽 위 x 좌표, 오른쪽 위 y 좌표
    a, b, c, d = map(int, input().split())
    #행 우선 순회
    for i in range(a, c):
        for j in range(b, d):
            if matrix[i][j] == 1: # 배열의 [i][j] 가 1이면 계속 순회
                continue
            else:
                matrix[i][j] = 1 # 위 조건이 아니면 배열의 [i][j]에 1 삽입
#100 x 100 배열을 순회하면서  1을 카운트 합니다.
cnt = 0
for i in range(100):
    for j in range(100):
        if matrix[i][j] == 1:
            cnt += 1
print(cnt)