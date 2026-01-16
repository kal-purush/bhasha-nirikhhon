package com.practice.interviewbit;

import java.util.ArrayList;
import java.util.Scanner;

public class FirstMissingInteger {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        ArrayList<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
            list.add(i, sc.nextInt());
        int ans = firstMissingInteger(list);
        System.out.println(ans);
    }

    private static int firstMissingInteger(ArrayList<Integer> list) {
        if (list.size() == 1 && list.get(0) == 1)
            return 2;
        int n = list.size();
        for (int i = 0; i < n; i++)
            if (list.get(i) <= 0 || list.get(i) > n)
                list.set(i, Integer.MAX_VALUE);

        for (int i = 0; i < n; i++){
            if (Math.abs(list.get(i)) != Integer.MAX_VALUE){
                int idx = Math.abs(list.get(i));
                if (!(list.get(idx-1) < 0)){
                    list.set(idx-1, list.get(idx-1)*-1);
                }
            }
        }
        int i;
        for ( i = 0; i < n; i++)
            if (list.get(i) > 0)
                return i+1;

            return i+1;
    }
}