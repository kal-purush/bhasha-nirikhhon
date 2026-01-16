package com.zubko.lectures.lecture_28;

import java.util.ArrayList;
import java.util.List;

//нарцистическое число
public class NumbersApp {
    public static void main(String[] args) {
        int num = 153;
        System.out.println(func(num));

    }

    private static boolean func(int num) {
        String s = "" + num;
        char[] digits = s.toCharArray();
        int size = digits.length;
        List<String> strDigits = new ArrayList<>();
        for (char c : digits)
            strDigits.add("" + c);

        int sum = strDigits
                .stream()
                .map(d -> (int) Math.pow(Integer.valueOf("" + d), size))
                .reduce(0, Integer::sum);

        return num == sum;
    }
}