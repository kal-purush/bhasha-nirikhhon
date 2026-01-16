def get_ways(x, y, dp, board, size):
    print(len(dp))
    if (x, y) in dp:
        return dp[(x, y)]

    if y == x == size-1:
        return 1

    distance = board[y][x]
    down = get_ways(x, y+distance, dp, board, size) if y + distance < size else 0
    right = get_ways(x+distance, y, dp, board, size) if x + distance < size else 0

    dp[(x, y)] = down + right;
    return down + right


def main():
    size = int(input())
    board = [list(map(int, input().split())) for _ in range(size)]

    print(get_ways(0, 0, dict(), board, size))


if __name__ == '__main__':
    main()