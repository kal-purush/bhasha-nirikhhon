package alg.search;

import edu.princeton.cs.introcs.StdRandom;
import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * @author Japhy
 * @since 2022/3/3 15:12
 */
public class BinarySearch {

    public static int rank(int[] nums, int x, int lo, int hi) {
        int mid = lo + (hi - lo) / 2;
        if (nums[mid] == x) {
            return mid;
        }
        if (nums[mid] > x) {
            return rank(nums, x, lo, mid - 1);
        } else {
            return rank(nums, x, mid + 1, hi);
        }
    }

    public static void main(String[] args) {
        int[] ints = IntStream.range(1, 100).toArray();
        StdRandom.shuffle(ints);
        Arrays.sort(ints);
        System.out.println(rank(ints, 67, 0, ints.length - 1));
    }

}