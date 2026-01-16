package homework.day11;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.floor;

public class MyDoubles {
    public static void main(String[] args) {
        List<Double> doubles = new ArrayList<>(Arrays.asList(33.42, 34.3, 0.79, 2.3426, 6.8, 13.24, 5.5, 769.9));

        for (double doubleN : doubles) {
            System.out.print(doubleN + " ");
        }
        System.out.println();

        double mult = 1;
        for (double doubleN : doubles) {
            mult *= doubleN;
        }
        System.out.println(mult);

        double sumFract = 0;
        for (double doubleN : doubles) {
            double myint = floor(doubleN);
            double fraction = doubleN - myint;
            sumFract += fraction;
        }
        System.out.printf("%f", sumFract);
        System.out.println();

        for (int i = 0; i < doubles.size(); i++) {
            int integerPart = (int) floor(doubles.get(i));
            System.out.print(integerPart + " ");
        }
    }
}