N = int(input()) # 강의실의 가로, 세로 열의 개수

room = [list(map(int, input().split())) for _ in range(N)]

others = []
for i, row in enumerate(room):
    for j, element in enumerate(row):
        if element == 5:
            pro = [i, j]
        elif element == 2:
            kyu = [i, j]
        elif element == 1:
            others.append([i, j])

# 성규와 교수님의 거리가 5 이상
# 교수님과 성규를 꼭짓점으로 하는 축에 평행한 직사각형 안에, 교수님을 제지해줄 성규가 아닌 학생이 세 명 이상 있어야 한다.
# 교수님과 성규가 같은 행 혹은 같은 열의 책상에 앉아있다면 교수님과 성규를 잇는 선분 상에 성규가 아닌 학생이 세 명 이상 있어야 한다.

# 교수님과 성규의 거리
distance = ((pro[0]-kyu[0])**2 + (pro[1]-kyu[1])**2)**0.5

# 교수님과 성규를 꼭짓점으로 하는 축에 평행한 직사각형 안에, 교수님을 제지해줄 성규가 아닌 학생이 세 명 이상 있어야 한다
# 교수님과 성규가 같은 행이나 열일 때 동일 선분 상에 학생이 세 명 이상 있어야 하는 조건도 마나족
cnt = 0
for i in range(min(pro[0], kyu[0]), max(pro[0], kyu[0])+1):
    for j in range(min(pro[1], kyu[1]), max(pro[1], kyu[1])+1):
        if room[i][j] == 1:
            cnt += 1

if distance >= 5 and cnt >=3:
    print(1)
else:
    print(0)