import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CalculateExpression {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        List<Double> numbers = new ArrayList<Double>();
        String strScanner;
        while (true){
            strScanner = scanner.nextLine();
            if(strScanner.isEmpty() || strScanner == null){
                break;
            }
            numbers.add(Double.parseDouble(strScanner));
        }
        double fR = calculatesFirstResult(numbers.get(0), numbers.get(1), numbers.get(2));
        double sR = calculateSecondResult(numbers.get(0), numbers.get(1), numbers.get(2));
        double result = Math.abs((numbers.get(0) + numbers.get(1) + numbers.get(2)) / 3 - (fR + sR) / 2);

        System.out.printf("F1 result: %.2f; F2 result: %.2f; Diff: %.2f",fR,sR,result);
    }

    public static double calculatesFirstResult(double a, double b, double c){
        double power = (a + b + c)/ Math.sqrt(c);
        double expresion = (Math.pow(a,2) + Math.pow(b,2))/(Math.pow(a,2) - Math.pow(b,2));
        double result = Math.pow(expresion,power);

        return result;
    }

    public static double calculateSecondResult(double a, double b, double c) {
        double power = a - b;
        double expresion = Math.pow(a, 2) + Math.pow(b, 2) - Math.pow(c, 3);
        double result = Math.pow(expresion, power);

        return result;
    }
}
