'''
입력받은 배열에서
가로 또는 세로로 자를 때마다 그 자르는 선을 기준으로 a,-a입력
이후 가장 빈도수가 높은 숫자의 빈도 구하기
'''

W, H = map(int, input().split()) #가로, 세로길이
N = int(input()) #자르는 횟수
paper = [[0]*W for _ in range(H)] #종이배열 선언
for _ in range(N):
    standard, n = map(int, input().split()) # 자르는 기준 선 입력
    frequency = {}
    for h in range(H):
        for w in range(W):
            if standard == 0:  # 가로자르기
                if h < n:
                    paper[h][w] += 0
                else:
                    paper[h][w] += 1

            elif standard == 1:  # 세로자르기
                if w < n:
                    paper[h][w] += n
                else:
                    paper[h][w] += -n

            if paper[h][w] in frequency:
                frequency[paper[h][w]] += 1
            else:
                frequency[paper[h][w]] = 1

print(max(frequency.values()))