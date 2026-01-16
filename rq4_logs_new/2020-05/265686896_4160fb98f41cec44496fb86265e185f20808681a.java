package day1;

public class PalindromeNumber {

    public static void main(String[] args) {

        System.out.println(isPalindrome(12321));
    }

    public static boolean isPalindrome(long num){

        long remain = num; //12321
        long rev =0;

        while(remain != 0){
            long digit = remain % 10;

            rev = rev * 10 + digit;

            remain /= 10;
        }

        return num == rev;

    }
}