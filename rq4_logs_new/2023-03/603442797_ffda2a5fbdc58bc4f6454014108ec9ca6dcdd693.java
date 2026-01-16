package classwork.day11;

import java.util.*;

public class Iterate {
    public static void main(String[] args) {
        Set<String> mySet = new HashSet<>();
        String[] array = "Мама мыла раму мыла ".split(" ");

        for (String s : array) {
            mySet.add(s);
        }

        for (String s : mySet) {
            System.out.println(s);

        }

        Iterator<String> iterator = mySet.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

    }
}