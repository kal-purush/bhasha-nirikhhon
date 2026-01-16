package org.learning24.java8.ImperativeVsDeclarative;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Removing duplicates from a list of integers
 */
public class ImperativeVsDeclarativeExample2 {
    public static void main(String[] args) {

        List<Integer> integerList = Arrays.asList(1,1,2,3,4,3,5,5,6,7,7,8,7);

        /**
         * Imperative
         */

        List<Integer> uniqueList = new ArrayList<>();
        for (Integer integer : integerList){
            if(!uniqueList.contains(integer)){
               uniqueList.add(integer);
            }
        }
        System.out.println("Imperative: "+uniqueList);

        /**
         * Declarative
         */

        List<Integer> uniqueList2 = integerList
                .stream()
                .distinct()
                .collect(Collectors.toList());

        System.out.println("Declarative: "+uniqueList2);

    }
}