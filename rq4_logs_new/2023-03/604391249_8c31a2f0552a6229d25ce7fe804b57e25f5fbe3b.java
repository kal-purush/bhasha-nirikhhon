package com.abneco.delivery.utils;

public class NameFormatter {

    public static String formatToCapitalLetter(String fullName) {
        String[] names = fullName.split(" ");
        StringBuilder result = new StringBuilder();

        for (String name : names) {
            String firstLetter = name.substring(0, 1);
            String rest = name.substring(1);
            String capitalized = firstLetter.toUpperCase() + rest;
            result.append(capitalized).append(" ");
        }
        return result.toString().trim();
    }

    private NameFormatter() {
    }
}