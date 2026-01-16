package lv.acodemy.classroom;

public class PlayWithMethods {
    public static void main(String[] args) {
        greeting("Vitalijs");

        // Method that returns something
        var x = add(10, 20);
        System.out.println(x);

        System.out.println(add(30, 40));

        // Check if number is even
        System.out.println(isEven(10));
        System.out.println(isEven(11));
        System.out.println(isEven(500));
        System.out.println(isEven(13));

        System.out.println(findMax(40, 30));
        System.out.println(findMax(20, 10));
        System.out.println(findMax(1, 0));

        // Check if word is palindrome
        System.out.println(isPalindrome("kayak"));
        System.out.println(isPalindrome("acodemy"));

    }

    public static void greeting(String name) {
        System.out.println("Hello, " + name + "!");
    }

    public static int add(int firstNumber, int secondNumber) {
        return firstNumber + secondNumber;
    }

    public static boolean isEven(int number) {
        return number % 2 == 0;
    }

    public static int findMax(int numberOne, int numberTwo) {
        return Math.max(numberOne, numberTwo);
    }

    // Palindrome (kayak, civic, rotator, madam etc.)

    public static boolean isPalindrome(String word) {
        String reversedWord = "";
        for (int i = word.length() - 1; i >= 0; i--) {
            reversedWord = reversedWord + word.charAt(i);
        }
        return word.equals(reversedWord);
    }
}