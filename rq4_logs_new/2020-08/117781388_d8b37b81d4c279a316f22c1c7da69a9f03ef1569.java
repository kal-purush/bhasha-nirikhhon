package BreadthFirstSearch.TwoDArrays;

import javafx.util.Pair;

import java.util.ArrayDeque;
import java.util.Queue;

public class RottingOranges {

    int ROTTEN = 2;
    int FRESH = 1;

    public int orangesRotting(int[][] grid) {
        Queue<Pair<Integer, Integer>> queue = new ArrayDeque<>();

        // Step 1). build the initial set of rotten oranges
        int freshOranges = 0;
        int row = grid.length;
        int col = grid[0].length;

        // traverse 2d array
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                if (grid[i][j] == ROTTEN) {
                    queue.offer(new Pair(i, j));
                } else if (grid[i][j] == FRESH) {
                    freshOranges++;
                }
            }
        }

        // Mark the round / level, _i.e_ the ticker of timestamp
        queue.offer(new Pair(-1, -1));

        // Step 2). start the rotting process via BFS.
        int minutesElapsed = -1;
        int[][] directions = {{-1, 0}, {0, 1}, {1, 0}, {0, -1}};

        while (!queue.isEmpty()) {
            Pair<Integer, Integer> p = queue.poll();
            int r = p.getKey();
            int c = p.getValue();

            if (r == -1) {
                // We finish one round of processing
                minutesElapsed++;
                // to avoid the endless loop
                if (!queue.isEmpty()) {
                    queue.offer(new Pair(-1, -1));
                }
            } else {
                // this is a rotten orange
                // then it would contaminate its neighbors
                for (int[] d : directions) {
                    int neighborRow = r + d[0];
                    int neighborCol = c + d[1];
                    if (inBound(grid, new Pair(neighborRow, neighborCol))) {
                        if (grid[neighborRow][neighborCol] == FRESH) {
                            // this orange would be contaminated
                            grid[neighborRow][neighborCol] = ROTTEN;
                            freshOranges--;
                            // this orange would then contaminate other oranges
                            queue.offer(new Pair(neighborRow, neighborCol));
                        }
                    }
                }
            }
        }

        // return elapsed minutes if no fresh orange left
        return freshOranges == 0 ? minutesElapsed : -1;
    }

    public boolean inBound(int[][] grid, Pair<Integer, Integer> p) {
        int row = grid.length;
        int col = grid[0].length;
        int x = p.getKey();
        int y = p.getValue();
        return x >= 0 && x < row && y >= 0 && y < col;
    }
}