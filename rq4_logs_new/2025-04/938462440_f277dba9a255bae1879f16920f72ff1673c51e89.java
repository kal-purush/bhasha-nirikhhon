package strucky.hashing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExclusiveItemsHashing {

    //  Time complexity: O(n + m + x + y) -> O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(n)
    //  Pattern: Hashing with sets
    public static List<Integer> exclusiveItems(List<Integer> a, List<Integer> b) {
        Set<Integer> setA = new HashSet<>(a);
        Set<Integer> setB = new HashSet<>(b);
        List<Integer> listOfExclusiveItems = new ArrayList<>();

        for (int num : a) {
            if (! setB.contains(num)) {
                listOfExclusiveItems.add(num);
            }
        }

        for (int num : b) {
            if (! setA.contains(num)) {
                listOfExclusiveItems.add(num);
            }
        }

        return listOfExclusiveItems;
    }
}