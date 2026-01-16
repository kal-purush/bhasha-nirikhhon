//https://leetcode.com/problems/base-7/
package algorithms.easy;

import java.math.BigInteger;

public class Base7 {
    public static void main(String[] args) {
        Base7 base7 = new Base7();
        System.out.println("Output:\t" + base7.convertToBase7Fastest(-43));
        System.out.println("Output:\t" + base7.convertToBase7NonOptimized(31));
        System.out.println("Output:\t" + base7.convertToBase7Optimized(121));
    }

    public String convertToBase7Fastest(int num) {
        return Integer.toString(num, 7);
    }

    public String convertToBase7NonOptimized(int num) {
        if (num == 0) return "0";
        StringBuilder sb = new StringBuilder();
        boolean sign = num > 0 ? true : false;
        num = Math.abs(num);
        while (num > 0) {
            int mod = num % 7;
            num /= 7;
            sb.append(mod);
        }
        if (!sign) sb.append("-");
        return sb.reverse().toString();
    }

    public String convertToBase7Optimized(int num) {
        BigInteger m = new BigInteger("" + num);
        return m.toString(7);
    }
}