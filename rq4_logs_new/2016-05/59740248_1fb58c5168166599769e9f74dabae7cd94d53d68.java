import java.util.Scanner;

public class Calculator {
    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        System.out.println("Please, input first number.");
        Double firstValue = getDigit(scanner);

        System.out.println("Please, input arithmetic symbol *,-,*,/");
        String operator = scanner.next();

        while (!operator.equals("+") && !operator.equals("-") && !operator.equals("*") && !operator.equals("/")) {
            System.out.println("please input *,-,*,/ only");
            operator = scanner.next();
        }

        System.out.println("Please, input second number.");
        Double secondValue = getDigit(scanner);
        scanner.close();

        MathOperation operation = MathOperation.getOperation();
        System.out.println(operation.calculate(firstValue, secondValue));

    }

    private static double getDigit(Scanner scanner) {
        double inputDigit;
        do {
            try {
                inputDigit = scanner.nextDouble();
                break;
            } catch (Exception e) {
                System.out.println("Please, input digits only");
                scanner.next();
            }
        } while (true);
        return inputDigit;
    }
}