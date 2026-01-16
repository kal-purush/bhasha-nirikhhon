package ARRAY;

import com.sun.istack.internal.NotNull;

public class SumArray {

    public static void main (String [] args){
        double [] numbers = new double[3];
//        numbers[0] = 5;
//        numbers[1] = 10;
//        numbers[2] = 20;
        System.out.println(sum(numbers));
    }

    public static double sum(double[] numbers) {
        double total = 0;

        if (numbers.length==0) {
            return 0;
        } else {
            for (int i=0;i<numbers.length;i++) {
                total = total + numbers[i];
            }
        }
        return total;
    }
}


