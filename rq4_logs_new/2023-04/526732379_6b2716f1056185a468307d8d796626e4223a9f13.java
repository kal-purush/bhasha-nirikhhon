/*
43. Multiply Strings
Medium
6.1K
2.7K
Companies
Given two non-negative integers num1 and num2 represented as strings, return the product of num1 and num2, also represented as a string.

Note: You must not use any built-in BigInteger library or convert the inputs to integer directly.



Example 1:

Input: num1 = "2", num2 = "3"
Output: "6"
 */
public class MultiplyStrings {
        public String multiply(String num1, String num2) {
            return String.valueOf(getNumber(num1) * getNumber(num2));
        }

        private long getNumber(String nums) {
            long number = 0;
            for(int i=nums.length() - 1; i >= 0; i--) {
                char digit = nums.charAt(i);
                long num = 0;
                switch(digit) {
                    case '0' :
                        num = 0;
                        break;
                    case '1' :
                        num = 1;
                        break;
                    case '2' :
                        num = 2;
                        break;
                    case '3' :
                        num = 3;
                        break;
                    case '4' :
                        num = 4;
                        break;
                    case '5' :
                        num = 5;
                        break;
                    case '6' :
                        num = 6;
                        break;
                    case '7' :
                        num = 7;
                        break;
                    case '8' :
                        num = 8;
                        break;
                    case '9' :
                        num = 9;
                        break;
                }
                for(int j=nums.length() - 1; j > i; j--) {
                    num *= 10;
                }
                number += num;
            }
            System.out.println(number);
            return number;
        }
}