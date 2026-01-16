package org.learning24.java8.FunctionalInterfaces;

import java.util.function.Predicate;

public class PredicateExample {

    static Predicate<Integer> integerPredicate = (a) -> {
        return a % 2 ==0;
    };
    /**
     * single line statement simplification
     */
    static Predicate<Integer> integerPredicate1 = (a) ->
            a % 2 ==0;
    static Predicate<Integer> integerPredicate2 = (a) ->
            a % 5 ==0;
    public static void predicateAnd(){
        System.out.println("Predicate AND chaining result: "+integerPredicate1.and(integerPredicate2).test(9));
    }

    public static void predicateOR(){
        System.out.println("Predicate OR chaining result: "+integerPredicate1.or(integerPredicate2).test(9));
    }

    public static void predicateNegate(){
        System.out.println("Predicate Negate chaining result: "+integerPredicate1.or(integerPredicate2).negate().test(9));
    }
    public static void main(String[] args) {

        System.out.println(integerPredicate.test(3));
        System.out.println(integerPredicate1.test(4));
        predicateAnd();
        predicateOR();
        predicateNegate();

    }
}