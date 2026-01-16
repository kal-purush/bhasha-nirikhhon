T = int(input())

dr = [-1,-1,-1]
dc = [0,-1,1]

def queen(position,n) :
    global cnt 
    if position == n :
        cnt += 1 
        return 
    if position == 0 :
        for i in range(n) :
            grid[0][i] = 1 
            queen(position+1,n)
            grid[0][i] = 0
    else :
        for i in range(n) :
            for d in range(3) :
                nr = position + dr[d]
                nc = i + dc[d]

                if 0<=nr <n and 0<=nc<n :
                    if grid[nr][nc] :
                        break
            else :
                grid[position][i] = 1 
                queen(position+1,n)
                grid[position][i] = 0
    
for t in range(1,T+1) :
    n = int(input())

    grid = [[0]*n for _ in range(n)]

    cnt = 0
    queen(0,n)
    print(f'#{t} {cnt}')