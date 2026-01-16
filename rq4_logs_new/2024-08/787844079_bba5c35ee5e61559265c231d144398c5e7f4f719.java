//https://leetcode.com/problems/distribute-candies/
package algorithms.easy;

import java.util.HashSet;
import java.util.Set;

public class DistributeCandies {
    public static void main(String[] args) {
        DistributeCandies candies = new DistributeCandies();
        System.out.println("Output:\t" + candies.distributeCandies(new int[]{1, 1, 2, 2, 3, 3}));
        System.out.println("Output:\t" + candies.distributeCandies(new int[]{1, 1, 2, 3}));
        System.out.println("Output:\t" + candies.distributeCandies(new int[]{6, 6, 6, 6}));

    }

    public int distributeCandies(int[] candyType) {
        int n = candyType.length / 2;
        Set<Integer> set = new HashSet<>();
        for (int candy : candyType)
            set.add(candy);
        return Math.min(n, set.size());
    }
}