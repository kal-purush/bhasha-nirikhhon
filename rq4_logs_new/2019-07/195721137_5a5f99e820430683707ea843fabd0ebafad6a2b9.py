while True:
    try:
        K = int(input())

        if K == 0:
            break

        N, M = input().split()
        N = int(N)
        M = int(M)

        for i in range(K):
            X, Y = input().split()
            X = int(X)
            Y = int(Y)

            if X == N or Y == M:
                print('divisa')
            elif X < N and Y > M:
                print('NO')
            elif X > N and Y > M:
                print('NE')
            elif X < N and Y < M:
                print('SO')
            elif X > N and Y < M:
                print('SE')
                
    except EOFError:
        break