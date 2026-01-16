package ru.innopolis.attestations.attestation01;

public final class Utils {

    private Utils() {}

    public static boolean containsDigits(String string) {
        for (char chr : string.toCharArray()) {
            if (Character.isDigit(chr)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsSymbol(String string, char symbol) {
        for (char chr : string.toCharArray()) {
            if (chr == symbol) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsLetters(String string) {
        for (char chr : string.toCharArray()) {
            if (Character.isLetter(chr)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsLettersOnly(String string) {
        for (char chr : string.toCharArray()) {
            if (!Character.isLetter(chr)) {
                return false;
            }
        }
        return true;
    }

}