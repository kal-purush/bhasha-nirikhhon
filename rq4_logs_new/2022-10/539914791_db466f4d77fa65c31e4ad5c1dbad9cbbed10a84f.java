package org.example.algo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ValidateSubSequence {

    public static boolean isValidSubsequence(List<Integer> array, List<Integer> sequence) {
        int pos = 0;
        int elements = sequence.size();
        Iterator<Integer> iterator = sequence.iterator();
        while(iterator.hasNext()) {
            int s = iterator.next();
            for(int i = pos; i < array.size(); i++) {
                System.out.println("elements="+elements+" pos="+pos+" s="+s+" array.get(i="+i+")="+array.get(i));
                if(s == array.get(i)) {
                    elements--;
                    pos = i+1;
                }
            }
        }
        if(elements <=0) return true;
        return false;
    }

    public static void main(String[] args) {
        List array = List.of(5, 1, 22, 25, 6, -1, 8, 10);
        List sequence = List.of(5, 1, 22, 22, 25, 6, -1, 8, 10);
        System.out.println(ValidateSubSequence.isValidSubsequence(array, sequence));

        List array2 = List.of(1, 1, 1, 1, 1);
        List sequence2 = List.of(1, 1, 1);
        System.out.println(ValidateSubSequence.isValidSubsequence(array2, sequence2));
    }
}