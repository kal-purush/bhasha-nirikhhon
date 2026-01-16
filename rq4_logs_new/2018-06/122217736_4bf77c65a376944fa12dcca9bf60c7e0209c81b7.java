package com.collections.java9Features.java9SafeVarArgs;
import java.util.ArrayList;
import java.util.List;

/** It is an annotation which applies on a method or constructor that takes varargs parameters.
 * It is used to ensure that the method does not perform unsafe operations on its varargs parameters.
 * It was included in Java7 and can only be applied on
 * Final methods
 * Static methods
 * Constructors
 * From Java 9, it can also be used with private instance methods.
 **/

public class Java9SafeVarArgs {

    // Applying @SaveVarargs annotation
    @SafeVarargs
    private void display(List<String>... products) { // Not using @SaveVarargs
        for (List<String> product : products) {
            System.out.println(product);
        }
    }
    public static void main(String[] args) {
        Java9SafeVarArgs p = new Java9SafeVarArgs();
        List<String> list = new ArrayList<String>();
        list.add("Laptop");
        list.add("Tablet");
        p.display(list);
    }
}