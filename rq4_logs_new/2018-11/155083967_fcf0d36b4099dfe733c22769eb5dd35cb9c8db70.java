/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bi_function;

import java.util.function.BiFunction;

/**
 *
 * @author duminda
 */
public class BiFunctionTest {

public static void main(String args[]) 
    { 
        //R apply(T t, U u)
        //T: denotes the type of the first argument to the function
        //U: denotes the type of the second argument to the function
        //R: denotes the return type of the function
        // BiFunction to add 2 numbers 
        BiFunction<Integer, Integer, Integer> add = (a, b) -> a + b; 
  
        // Implement add using apply() 
        System.out.println("Sum = " + add.apply(2, 3)); 
  
        // BiFunction to multiply 2 numbers 
        BiFunction<Integer, Integer, Integer> multiply = (a, b) -> a * b; 
  
        // Implement add using apply() 
        System.out.println("Product = " + multiply.apply(2, 3)); 
        
        BiFunction<Integer, Integer, String> grade  = (a,b) -> (a+b)/2> 50 ? "pass" : "fail";
        
        System.out.println("grade test : " + grade.apply(50, 45));
    }     
}