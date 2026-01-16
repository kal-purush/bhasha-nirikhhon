package Generics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * array polymorphism; and List polymorphism.
 * Generic reference type, and Generic instance types must be identical.
 */
public class SuperTypeArrayAndSubTypes {

    public static void main(String[] args) {
        Integer[] numbers = {10, 2, 3};
        System.out.println(Arrays.toString(inputNumbers(numbers)));

        //reference generic type must identical to its object generic type;
        List<Integer> numbs = new ArrayList<>(); //a list of Integers objects
        //the following doesn't work! compiling error.
        //inputNumbers(numbs);//method argument:  List<Number> numbers
    }

    private static Number[] inputNumbers(Number[] numbers) {
        return numbers;
    }

    private static List<Number> inputNumbers(List<Number> numbers) {
        return numbers;
    }

}