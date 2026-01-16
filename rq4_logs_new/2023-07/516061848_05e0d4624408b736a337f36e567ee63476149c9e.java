package Easy.PalindromeNumber;

public class Solution {

    public boolean isPalindrome(int x) {
        int number = x;
        int reversed = 0;
        boolean result = true;

        if (x < 0) {
            result = false;
        } else if ( x>=0 ){
            while (number != 0) {
                int digit = number % 10;
                reversed = reversed * 10 + digit;
                number /= 10;
            }

            if (x == reversed) {
                result = true;
            } else {
                result = false;
            }
        }

        return result;
    }


    public static void main(String[] args) {

        int num = 1234, reversed = 0;

        while (num != 0) {

            int digit = num % 10;
            reversed = reversed * 10 + digit;

            num /= 10;
        }

        System.out.println("Reversed Number: " + reversed);


    }

}