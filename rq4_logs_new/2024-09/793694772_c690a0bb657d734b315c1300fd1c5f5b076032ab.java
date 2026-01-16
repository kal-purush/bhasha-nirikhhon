package LeetcodeContest.Weekly414;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ConvertDateToBinary {

    public static void printBinary(int[] binary, int id) {
        // Iteration over array
        for (int i = id - 1; i >= 0; i--)
            System.out.print(binary[i] + "");
    }

    public static String decimalToBinary(String str) {
        // Creating and assigning binary array size
        StringBuilder s = new StringBuilder();
        int num = Integer.parseInt(str);

        // Number should be positive
        while (num > 0) {
            s.append(num % 2);
            num = num / 2;
        }
        s.reverse();
        return s.toString();
    }

    public static String convertDateToBinary(String date) {
        String[] arrOfStr = date.split("-", 3);
        String[] newArr = new String[3];
        for (int i = 0; i < 3; ++i) {
            newArr[i] = decimalToBinary(arrOfStr[i]);
        }
        return String.join("-", newArr);
    }

    public static void main(String[] args) {

        String date = "2080-02-29";
        System.out.println(convertDateToBinary(date));
    }
}