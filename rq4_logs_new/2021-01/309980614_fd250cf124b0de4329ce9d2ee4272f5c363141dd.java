package com.java15.example.generics;

import java.util.List;

public class WildcardGuidelines {

    /**
     * Wildcard Guidelines:
     * An "in" variable is defined with an upper bounded wildcard, using the 'extends' keyword.
     * An "out" variable is defined with a lower bounded wildcard, using the 'super' keyword.
     * In the case where the "in" variable can be accessed using methods defined in the Object class, use an unbounded wildcard.
     * In the case where the code needs to access the variable as both an "in" and an "out" variable, do not use a wildcard.
     */
    public void copy(List<? extends Number>in, List<? super Integer> out){
        for(var var : in){
            if(var instanceof Integer i){
                out.add(i);
            }
        }
    }

    /**
     * 'copy(List<? extends Number>, List<? super Integer>)' clashes with 'copy(List<?>, List<? super Integer>)'; both methods have same erasure
     * @param in
     * @param out
     */
    /*public void copy(List<?>in, List<? super Integer> out){
        for(var var : in){
            if(var instanceof Integer i){
                out.add(i);
            }
        }
    }*/

    /**
     * 'copy(List<? extends Number>, List<? super Integer>)' clashes with 'copy(List<Object>, List<? super Integer>)'; both methods have same erasure
     * @param in
     * @param out
     */
    /*public void copy(List<Object>in, List<? super Integer> out){
        for(var var : in){
            if(var instanceof Integer i){
                out.add(i);
            }
        }
    }*/

}