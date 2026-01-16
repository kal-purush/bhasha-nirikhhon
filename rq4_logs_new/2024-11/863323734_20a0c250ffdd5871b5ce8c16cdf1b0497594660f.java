import java.util.*;

class Solution {
    public int slidingPuzzle(int[][] board) {
        final int[][] directions = {{0, 1}, {1, 0}, {0, -1}, {-1, 0}};
        String goal = "123450";
        StringBuilder startBuilder = new StringBuilder();
        
        // Convert the 2D board to a single string
        for (int[] row : board) {
            for (int cell : row) {
                startBuilder.append(cell);
            }
        }
        String start = startBuilder.toString();

        if (start.equals(goal)) {
            return 0; // Already solved
        }

        Queue<String> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();
        queue.offer(start);
        visited.add(start);

        int moves = 0;
        while (!queue.isEmpty()) {
            int size = queue.size();
            moves++;
            for (int i = 0; i < size; i++) {
                String current = queue.poll();
                int zeroIndex = current.indexOf('0');
                int x = zeroIndex / 3, y = zeroIndex % 3;

                for (int[] direction : directions) {
                    int newX = x + direction[0];
                    int newY = y + direction[1];
                    if (newX >= 0 && newX < 2 && newY >= 0 && newY < 3) {
                        char[] nextState = current.toCharArray();
                        int swapIndex = newX * 3 + newY;
                        // Swap '0' with the target position
                        char temp = nextState[zeroIndex];
                        nextState[zeroIndex] = nextState[swapIndex];
                        nextState[swapIndex] = temp;

                        String next = new String(nextState);
                        if (next.equals(goal)) {
                            return moves;
                        }
                        if (visited.add(next)) {
                            queue.offer(next);
                        }
                    }
                }
            }
        }

        return -1; // Not solvable
    }
}