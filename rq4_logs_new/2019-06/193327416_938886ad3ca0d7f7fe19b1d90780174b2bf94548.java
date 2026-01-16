package main.java;

import java.util.ArrayList;
import java.util.Iterator;

public class ArrayListTask {

    /**
     * This method checks all of the array elements and replaces the relevant element if it's value is "Tree" .
     */

    public void replaceElement(ArrayList<String> stringArrayList) {
        for (String i : stringArrayList) {
            if (i.contentEquals("Tree")) {
                stringArrayList.set(stringArrayList.indexOf(i), "Three");
            }
        }
        System.out.println(stringArrayList);
    }

    /**
     * This method uses Iterator to delete all the elements of the array that can be divided by 3 without the excess.
     */

    public void deleteElement(ArrayList<Integer> integerArrayList) {
        Iterator<Integer> iterator = integerArrayList.iterator();
        while (iterator.hasNext()) {
            Integer number = iterator.next();
            if (number % 3 == 0) {
                iterator.remove();
            }
        }
        System.out.println(integerArrayList);
    }
}