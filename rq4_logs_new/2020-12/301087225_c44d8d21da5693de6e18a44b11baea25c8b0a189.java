import java.util.Scanner;

public class Tugas1 {
    public static void main(String[] args) {
        Scanner obj = new Scanner(System.in);

        String t = obj.nextLine().toUpperCase();
        System.out.print("#" + hexadecimal(t) + palindromString(t) + octal(t));

        char hc = String.valueOf(hexadecimal(t)).charAt(0);
        if (hc >= 'a' && hc <= 'z') {
            System.out.print("?");
        } else {
            System.out.print("!");
        }
    }
    static String halfText(String a) {
        String nt = a.replaceAll(" ", "");
        int tl = nt.length() / 2;
        nt = nt.substring(0, tl);
        return nt;
    }
    static String hexadecimal(String a) {
        int tl = a.length();
        int decimal = tl * tl;
        String hex = String.format("%x", decimal);
        return hex;
    }
    static String palindromString(String a) {
        String t = halfText(a);
        String palindrome = "";
        for (int i = t.length() - 2; i >= 0; i--) {
            palindrome += t.charAt(i);
        }
        String nt = t.concat(palindrome);
        return nt;
    }
    static String octal(String a) {
        int tl = a.length();
        String octal = String.format("%o", tl);
        return octal;
    }

}