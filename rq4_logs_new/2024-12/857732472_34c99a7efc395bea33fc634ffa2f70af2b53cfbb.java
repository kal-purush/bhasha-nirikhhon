package Collections_Set_53_54;


import java.util.*;

import static Collections_Set_53_54.SetHashSet.*;

public class Main {
    public static void main(String [] args) {

        int[] nums = {1, 2, 3, 2, 1, 4};
        System.out.println(findNumb(nums));


        Set<String> fruitsSet = new LinkedHashSet<>();

        fruitsSet.add("Apple");
        fruitsSet.add("Banana");
        fruitsSet.add("Cherry");
        fruitsSet.add("Apple");
        fruitsSet.add("Banana");

        System.out.println("Фрукты :");

        for (String fruit : fruitsSet) {
            System.out.println(fruit);
        }


        int[] numbers = {10, 20, 30, 40, 50};
        int target = 35;
        int[] result = findClosestNumbers(numbers, target);
        System.out.println(Arrays.toString(result));
    }

            public static int[] findClosestNumbers(int[] numbers, int target) {
                NavigableSet<Integer> set = new TreeSet<>();

                for (int i : numbers) {
                    set.add(i);
                }
                Integer minNum = set.lower(target);
                Integer maxNum = set.higher(target);

                if (minNum == null || maxNum == null){
                    return new int[]{};
                }

                return new int[]{minNum, maxNum};
            }
        }
