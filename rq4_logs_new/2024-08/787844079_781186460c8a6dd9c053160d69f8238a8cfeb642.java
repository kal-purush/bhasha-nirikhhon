//https://leetcode.com/problems/minimum-index-sum-of-two-lists/
package algorithms.easy;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class MinimumIndexSumOfTwoLists {
    public static void main(String[] args) {
        MinimumIndexSumOfTwoLists minIndex = new MinimumIndexSumOfTwoLists();
        System.out.println(
                "Output:\t" + minIndex.findRestaurant(new String[]{"Shogun", "Tapioca Express", "Burger King", "KFC"},
                        new String[]{"Piatti", "The Grill at Torrey Pines", "Hungry Hunter Steakhouse", "Shogun"}));
        System.out.println(
                "Output:\t" + minIndex.findRestaurant(new String[]{"Shogun", "Tapioca Express", "Burger King", "KFC"},
                        new String[]{"KFC", "Shogun", "Burger King"}));
        System.out.println("Output:\t" + minIndex.findRestaurant(new String[]{"happy", "sad", "good"},
                new String[]{"sad", "happy", "good"}));

    }

    public String[] findRestaurant(String[] list1, String[] list2) {
        Map<String, Integer> map = new HashMap<>();
        List<String> list = new ArrayList<>();
        int minIndex = Integer.MAX_VALUE;

        for (int i = 0; i < list1.length; i++)
            map.put(list1[i], i);

        for (int i = 0; i < list2.length; i++) {
            if (map.containsKey(list2[i])) {
                int currentIndex = map.get(list2[i]) + i;
                if (minIndex > currentIndex) {
                    list.clear();
                    list.add(list2[i]);
                    minIndex = currentIndex;
                } else if (minIndex == currentIndex) list.add(list2[i]);
            }
        }
        return list.toArray(new String[list.size()]);
    }
}