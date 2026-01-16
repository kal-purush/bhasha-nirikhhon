package javaclasses;

/**
 * Created by adamhurwitz on 2/27/16.
 */
public class SampleClass {
    static private final String LOG_TAG = SampleClass.class.getSimpleName();

    public static void main(String[] arg) {
        System.out.println("Answer is: " + stringMethod("Hello", "World"));
    }

    static int[] swapArray = {7, 9, 4};

    public static String stringMethod(String firstWord, String secondWord) {
        String stringToPrint;
        stringToPrint = firstWord + " " + secondWord;

        return stringToPrint;
    }
}