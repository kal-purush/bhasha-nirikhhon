package com.practice.interviewbit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MultiplyStrings {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] arr = br.readLine().split(" ");
        String num1 = arr[0];
        String num2 = arr[1];
        String ans = multiply(num1, num2);
        System.out.println(ans);
    }

    private static String multiply(String num1, String num2) {
        int[] result = new int[num1.length() + num2.length()];
        int i_n1 = 0;
        int i_n2 = 0;
        int len1 = num1.length();
        int len2 = num2.length();
        if (len1 == 0 || len2 == 0)
            return "0";
        for (int i = len1 - 1; i >= 0; i--){
            int n1 = num1.charAt(i)-'0';
            int carry = 0;
            i_n2 = 0;
            for (int j = len2 - 1; j>= 0; j--){
                int n2 = num2.charAt(j) - '0';
                int sum = n1 * n2 + result[i_n1 + i_n2] + carry;
                carry = sum / 10;
                result[i_n1 + i_n2] = sum%10;
                i_n2++;
            }
            if (carry > 0)
                result[i_n1 + i_n2] += carry;
            i_n1++;
        }
        int i = result.length - 1;
        while (i >= 0 && result[i] == 0){
            i--;
        }
        if (i == -1)
            return "0";
        StringBuilder ans = new StringBuilder("");
        while (i >= 0){
            ans.append(result[i--]);
        }
        return ans.toString();
    }
}