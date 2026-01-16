//https://leetcode.com/problems/range-addition-ii/description/
package algorithms.easy;

public class RangeAdditionII {
    public static void main(String[] args) {
        RangeAdditionII range = new RangeAdditionII();
        System.out.println("Output:\t" + range.maxCount(3, 3, new int[][]{{2, 2}, {3, 3}}));
        System.out.println("Output:\t" + range.maxCount(3, 3,
                new int[][]{{2, 2}, {3, 3}, {3, 3}, {3, 3}, {2, 2}, {3, 3}, {3, 3}, {3, 3}, {2, 2}, {3, 3}, {3, 3}, {3, 3}}));
        System.out.println("Output:\t" + range.maxCount(3, 3, new int[][]{}));
    }

    public int maxCount(int m, int n, int[][] ops) {
        for (int[] op : ops) {
            m = Math.min(m, op[0]);
            n = Math.min(n, op[1]);
        }
        return m * n;
    }
}