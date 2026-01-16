package strucky.recursion;

import java.util.*;

public class Recursion {

    //  Time complexity: O(n^2)
    //  Space complexity:
    //          Extra: O(n^2)
    //          Algo: O(n)
    //  Pattern: Recursion
    public static int getSumOfNumbers(List<Integer> numbers) {
        if (numbers.isEmpty()) return 0;

        int subListFrom = 1, subListTill = numbers.size();
        int firstNumber = numbers.getFirst();
        return (firstNumber + getSumOfNumbers(numbers.subList(subListFrom, subListTill)));
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    public static int getFactorial(int number) {
        if (number == 0) return 1;
        return number * getFactorial(number - 1);
    }

    //  Time complexity: O(n^2)
    //  Space complexity:
    //          Extra: O(n^2)
    //          Algo: O(n)
    public static int getSumOfLengths(List<String> stringList) {
        if (stringList.isEmpty()) return 0;
        int lengthOfFirstString = stringList.getFirst().length();
        int subListFrom = 1, subListTill = stringList.size();
        return lengthOfFirstString + getSumOfLengths(stringList.subList(subListFrom, subListTill));
    }

    //  Time complexity: O(n^2)
    //  Space complexity:
    //          Extra: O(n^2)
    //          Algo: O(n)
    public static String reverseString(String s) {
        if (s.isEmpty()) return "";
        return reverseString(s.substring(1)) + s.charAt(0);
    }

    //  Time complexity: O(n^2)
    //  Space complexity:
    //          Extra: O(n^2)
    //          Algo: O(n)
    //  Pattern:
    public static boolean isPalindrome(String s) {
        if (s.length() <= 1) return true;   //  Empty or length of 1.
        char firstChar = s.charAt(0);
        char lastChar = s.charAt(s.length() - 1);

        if (firstChar != lastChar) return false;

        return isPalindrome(s.substring(1, s.length() - 1));
    }

    //  Time complexity: O(2^n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(1)
    public static int getFibonacci(int n) {
        if (n <= 1) return n;
        return getFibonacci(n - 1) + getFibonacci(n - 2);
    }
}