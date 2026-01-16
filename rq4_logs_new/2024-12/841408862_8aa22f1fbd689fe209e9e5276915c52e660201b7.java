package org.example.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class ArrayExample {
    public static void main(String[] args) {
        int[] arr = {1,9,3,4,7,6,7,8,9,10,1,2,3,4};
        System.out.println();
//        for(int i : arr){
//            System.out.println(i);
//        }
        ArrayList<Integer> al = new ArrayList<>();
        for( int j : arr){
            al.sort(Comparator.naturalOrder());
            al.add(j);
        }

        System.out.println(al);
      //  al.sort(Comparator.naturalOrder());
        System.out.println(al);

    }
}