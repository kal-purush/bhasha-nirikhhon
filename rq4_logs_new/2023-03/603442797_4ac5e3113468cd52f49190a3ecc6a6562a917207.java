package homework.day7.stringtask;

import java.util.Arrays;

public class NumbersInArraySt {

    public void MethodNumbersInArray(String text) {
        String[] numbersStringArray = text.split("\\D+");
        String numbersString = String.join("", numbersStringArray);
        char[] characters = numbersString.toCharArray();
        int[] digitals = new int[characters.length];
        for (int i = 0; i < characters.length; i++) {
            digitals[i] = (characters[i]-48);
        }
        System.out.println(Arrays.toString(digitals));
    }
}