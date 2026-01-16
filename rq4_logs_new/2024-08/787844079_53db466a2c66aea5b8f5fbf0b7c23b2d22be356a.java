//https://leetcode.com/problems/island-perimeter/
package algorithms.easy;


public class IslandPerimeter {
    public static void main(String[] args) {
        IslandPerimeter perimeter = new IslandPerimeter();
        System.out.println("Output:\t" + perimeter.islandPerimeter(
                new int[][]{{0, 1, 0, 0}, {1, 1, 1, 0}, {0, 1, 0, 0}, {1, 1, 0, 0}}));
        System.out.println("Output:\t" + perimeter.islandPerimeter(new int[][]{{1}}));
        System.out.println("Output:\t" + perimeter.islandPerimeter(new int[][]{{1, 0}}));
    }

    public int islandPerimeter(int[][] grid) {
        int answer = 0;
        int rows = grid.length;
        int columns = grid[0].length;

        if (grid == null || rows == 0 || columns == 0)
            return answer;

        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < grid[row].length; col++) {
                if (grid[row][col] == 1) {
                    answer += 4;
                    if (row > 0 && grid[row - 1][col] == 1)
                        answer -= 2;

                    if (col > 0 && grid[row][col - 1] == 1)
                        answer -= 2;
                }
            }
        }
        return answer;
    }
}