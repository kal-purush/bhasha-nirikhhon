package com.abneco.delivery.utils;

public class UpperCaseFormatter {

    public static String formatToCapitalLetter(String words) {
        String[] wordsList = words.split(" ");
        StringBuilder result = new StringBuilder();

        for (String word : wordsList) {
            String firstLetter = word.substring(0, 1);
            String rest = word.substring(1);
            String capitalized = firstLetter.toUpperCase() + rest;
            result.append(capitalized).append(" ");
        }
        return result.toString().trim();
    }

    private UpperCaseFormatter() {
    }
}