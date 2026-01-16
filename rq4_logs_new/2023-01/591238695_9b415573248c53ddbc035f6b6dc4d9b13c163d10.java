package com.csaba79coder.bookliquibase.domain.util;

public class ISBNValidator {

    public static boolean validISBN(String isbnNumber) {
        isbnNumber = isbnNumber.replace("-", "").trim();
        if (isbnNumber.length() == 10 && isbnNumber.matches("[0-9]+")) {
            int sum = 0;
            for(int i = 10; i > 0; i--) {
                if (isbnNumber.charAt(9) == 'X' && i == 1) {
                    sum += i * 10;
                } else {
                    sum += i * Character.getNumericValue(isbnNumber.charAt(10 - i));
                }
            }
            return sum % 11 == 0;
        }
        return false;
    }

}