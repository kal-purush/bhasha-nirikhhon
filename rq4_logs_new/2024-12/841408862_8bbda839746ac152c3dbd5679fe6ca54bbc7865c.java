package org.example;

public class JavaNonPrimitiveDataTypes {
    public static void main(String[] args) {
        /* java has 2 types data types
        * 1.Primitive
        * 2.Non Primitive Data types
        * under non primitive data types as follows String Arrays Custom Objects or there */
        String s  = new String("Sivaiah");
        System.out.println(s);//or we can declare directly
        String s1 = "Siva";
        System.out.println(s1);

        //Arrays a colletion of same data types

       int [] nums = new int[5];// one way of creating array , and it has fixed length we can't change in future
        System.out.println(nums[0]);
        int [] num = {1,2,3,4,5};//other way and index staring with zero 0
        System.out.println(num[4]);// output prints index value == 5
    }

}