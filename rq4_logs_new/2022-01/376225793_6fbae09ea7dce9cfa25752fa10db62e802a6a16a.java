import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class FrameRotation {
    @Test
    public void run() {
        int[] answer = solution(
                6,
                6,
                new int[][] {
                        {2,2,5,4},
                        {3,3,6,6},
                        {5,1,6,3}
                });
        for (int a : answer) {
            System.out.print(a + " ");
        }
    }

    public int[] solution(int rows, int columns, int[][] queries) {
        List<Integer> answer = new ArrayList<>();
        int[][] arr = new int[rows + 1][columns + 1];
        int[][] dirs = new int[][] {{1, 0}, {0, 1}, {-1, 0}, {0, -1}};

        for (int i = 1; i <= rows; i++) {
            for (int j = 1; j <= columns; j++) {
                arr[i][j] = (i - 1) * columns + j;
            }
        }

        for (int i = 0; i < queries.length; i++) {
            int y = queries[i][0], x = queries[i][1];
            int d = 0;
            int last, min;
            last = min = arr[y][x];

            while (true) {
                if (y == queries[i][0] && x == queries[i][1] + 1) {
                    break;
                }

                int ny = y + dirs[d][0];
                int nx = x + dirs[d][1];

                if (ny < queries[i][0] || queries[i][2] < ny || nx < queries[i][1] || queries[i][3] < nx) {
                    d = d + 1 > 3 ? 0 : d + 1;
                    ny = y + dirs[d][0];
                    nx = x + dirs[d][1];
                }

                arr[y][x] = arr[ny][nx];
                min = Math.min(min, arr[y][x]);
                y = ny;
                x = nx;
            }
            arr[y][x] = last;
            answer.add(min);
        }

        return answer.stream().mapToInt(i -> i).toArray();
    }
}