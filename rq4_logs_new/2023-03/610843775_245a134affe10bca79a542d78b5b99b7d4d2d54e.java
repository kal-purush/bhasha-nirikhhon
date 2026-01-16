/**
 * Напишите метод, который принимает на вход строку (String) и определяет
 * является ли строка палиндромом (возвращает boolean значение).
 */
package homework2;

public class Task1 {

    public static void main(String[] args) {
        String str = "Аргентина манит негра";
        str = str.toLowerCase();
        boolean A = palindrome(str);
        System.out.println(A);
    }

    private static boolean palindrome(String str) {
        var chars = str.toLowerCase().toCharArray();
        var left = 0;
        var right = chars.length - 1;
        while (left < right) {
            if (chars[left] != chars[right]) {
                return false;
            }
            do {
                left++;
            }
            while (left < right && chars[left] == ' ');
            do {
                right--;
            }
            while (right > left && chars[right] == ' ');
        }
        return true;
    }
}