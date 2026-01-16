package File_work.Containers;

import java.util.OptionalDouble;
import java.util.Scanner;

public class task_1 {
    static OptionalDouble sqrtUser(double x) {
        OptionalDouble result = OptionalDouble.empty();
        if (x >= 0) {
            result = OptionalDouble.of(Math.sqrt(x));
        }
        return result;
    }

    public static void main(String[] args) {
        OptionalDouble y;
        double x = new Scanner(System.in).nextDouble();
        y = sqrtUser(x);
        if (y.isPresent()) {
            System.out.printf("y = %6.3f%n", y.getAsDouble());
        } else {
            System.out.println("Error");
        }
    }
}