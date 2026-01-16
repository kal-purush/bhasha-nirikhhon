package cn.madf.leetCode;

import io.netty.handler.timeout.ReadTimeoutException;
import scala.util.Either;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author 烛影鸾书
 * @date 2020/9/23 21:30
 * @copyright© 2020
 */
public class P56_MergeRange {

    public static void main(String[] args) {
        int[][] m = new int[][]{{1, 3}, {2, 6}, {8, 10}, {15, 18}};
        int[][] r = merge(m);
        System.out.println(Arrays.deepToString(r));
    }

    public static int[][] merge(int[][] intervals) {
        if (intervals.length == 0) {
            return new int[0][0];
        }
        ArrayList<Integer[]> res = new ArrayList<>();
        Arrays.sort(intervals, (o1, o2) -> {
            if (o1[0] == o2[0]) {
                return o1[1] - o2[1];
            } else {
                return o1[0] - o2[0];
            }
        });

        res.add(new Integer[]{intervals[0][0], intervals[0][1]});
        for (int[] interval : intervals) {
            Integer[] lastRange = res.get(res.size() - 1);
            if (interval[0] <= lastRange[1]) {
                lastRange[1] = Math.max(lastRange[1], interval[1]);
            } else {
                res.add(new Integer[]{interval[0], interval[1]});
            }
        }
        int[][] resMatrix = new int[res.size()][2];
        for (int i = 0; i < res.size(); i++) {
            resMatrix[i][0] = res.get(i)[0];
            resMatrix[i][1] = res.get(i)[1];
        }
        return resMatrix;
    }
}