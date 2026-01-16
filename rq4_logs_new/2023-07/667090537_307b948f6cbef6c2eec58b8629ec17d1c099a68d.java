import java.util.Scanner;

public class MaxDigitFromNumber {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int number = scanner.nextInt();
        int maxDigit = number % 10;
        String temp = Integer.toString(number);
        int length = temp.length();
        int a = 10;
        int b = 1;
        for (int i = 0; i < length; i++) {
            if ((number % a) / b > maxDigit) {
                maxDigit = (number % a) / b;
            }
            a *= 10;
            b *= 10;
        }
        System.out.println(maxDigit);
    }
}