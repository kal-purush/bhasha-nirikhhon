package implementation.p2745;

import implementation.Implementation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws IOException {

        System.setIn(new FileInputStream(Implementation.PATH + "/p2745/data/3.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String[] data = br.readLine().split(" ");
        int result = BaseConversion.convert(data);
         System.out.print(result);
    }
}

class BaseConversion {

    public static int convert(String[] data) {

        return BaseConversion.convert(data[0], Integer.parseInt(data[1]));
    }

    private static int convert(String nums, int base) {

        int decimal = 0;
        for (int i = 0; i < nums.length(); ++i) {
            int num = Character.getNumericValue(nums.charAt(nums.length() - i - 1));
            decimal += (int) (num * Math.pow(base, i));
        }
        return decimal;
    }
}