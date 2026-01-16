package com.josh.code.general3;

import com.josh.tryout.util.Helper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Munish on 5/25/17.
 */
public class PhoneLetterCombination2 {

    public static String[] LETTERS = {"", "", "ABC", "DEF", "GHI", "JKL", "MNO", "PQRS", "TUV", "WXYZ"};

    public static void main(String[] args) {

        int[] digits = {2, 3};

        List<String> combinations = new ArrayList<String>();

        findCombinations(digits, combinations, 0, "");

        Helper.print(combinations);


    }


    private static void findCombinations(int[] digits, List combinations, int index, String prefix) {

        if (index < digits.length) {

            for (char c : LETTERS[digits[index]].toCharArray()) {

                findCombinations(digits, combinations, index + 1, prefix + c);

            }

        } else {

            combinations.add(prefix);

        }


    }


}