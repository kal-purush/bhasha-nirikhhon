#아이디어 DFS를 사용해서 서치한다
#BFS는 Breadth-First Search의 약자로 가까운 노드부터 우선적으로 탐색하는 알고리즘. Queue를 사용해서 방문 노드를 visited 처리를 한다
#DFS는 Depth-First Search의 약자로 깊은 부분을 우선적으로 탐색하는 알고리즘
#스택 자료구조(혹은 재귀 함수)를 이용하며, LIFO의 특징을 지님. 마지막에 들어간게 처음 나가는 깊이 탐색과 개념이 같음 그래서 DFS는 스택 자료구조로 활용 가능
class Solution:
    def numIslands(self, grid: List[List[str]]) -> int:
        
        # 인풋이 2차원인 경우 필요한 지식 정리
        print("Row 값 :",len(grid)) # row값으로 4 출력: 가장 바깥쪽 차원'[ ]'을 기준으로 내부 [] 덩어리 갯수 출력
        print("grid[0]:", grid[0])

        print("Col 값:", len(grid[0])) # col 값으로 5 출력 : 다음 안쪽 차원 '[]'을 기준으로 내부 길이 출력
        print("grid[0][4]:", grid[0][4])
        print(100 and 'hi') # 앞의 100은 트루로 인식
        print("----사전 지식 정리 끝----")

        row, col = len(grid), len(grid) and len(grid[0]) # row 및 col 할당 / 직관적 이해를 위해 row,col로 정의함
                                                     # len(grid) 행이 존재한다면 True and 열의 길이를 반환하는 코딩 방식
            
        count = 0
        for r in range(row): # 0 ~ 4
            for c in range(col): # 0 ~ 5
                if grid[r][c] == '1':
                    self.dfs(grid, r, c)
                    count += 1
            #print(grid[r])
        return count
                    
    def dfs(self, grid, r, c):
        #i,j가 음수이거나, 주어진 차원 길이를 넘어가거나, 현재 선택한 값이 1이 아닌 경우(땅이 아니고 물인 경우)
        if r<0 or c<0 or r>=len(grid) or c>=len(grid[0]) or grid[r][c] != '1':
            return # DFS 수행 안함

        grid[r][c] = '#' #방문한 곳에 #으로 표시하여, 헷갈리지 않게 하기 위함
        
        self.dfs(grid, r+1, c) # 상하 조절 row
        self.dfs(grid, r-1, c) # 상하 조절 row
        self.dfs(grid, r, c+1) # 좌우 조절 col
        self.dfs(grid, r, c-1) # 좌우 조절 col