package Calc;

import java.util.Scanner;

/**
 * Выходных параметров нет, метод осуществляет вывод в
 * консоль результатов расчетов. При этом позволяет выбрать тип
 * проводимой операции.
 */
public class Calculator {
    public void start(){
        Scanner scanner = new Scanner(System.in);
        double res = 0;

        System.out.println("Выберете операцию (+, -, *, /):");

        String sOper = scanner.next();
        char cOper = sOper.charAt(0);

        System.out.println("Введите первое число:");
        double num1 = scanner.nextDouble();

        System.out.println("Введите второе число:");
        double num2 = scanner.nextDouble();

        switch (cOper) {
            case '+':
                res = num1 + num2;
                break;
            case '-':
                res = num1 - num2;
                break;
            case '*':
                res = num1 * num2;
                break;
            case '/':
                res = num1 / num2;
                break;
            default:
                System.out.println("Некорректная операция");
                break;
        }

        System.out.printf("%+5.4f", res);
        scanner.close();
    }

}