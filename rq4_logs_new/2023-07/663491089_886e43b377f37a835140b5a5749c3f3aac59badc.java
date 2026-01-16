// Реализовать простой калькулятор

import java.util.Scanner;

public class program3 {
    public static void main(String[] args) {
        
        Scanner in = new Scanner(System.in);

        System.out.print("Введите первое число: ");
        Integer firstNumber = in.nextInt();
        System.out.print("Введите действие ('+' или '-' или '*' или '/'):  ");
        String action = in.next();
        System.out.print("Введите второе число: ");
        Integer secondNumber = in.nextInt();
        in.close();

        Integer result = 0;
        Integer sum = 0;
        Integer minus = 0;
        Integer multiply = 0;
        Integer division = 0;

        sum = action.compareTo("+");
        minus = action.compareTo("-");
        multiply = action.compareTo("*");
        division = action.compareTo("/");
        if (sum == 0){
            result = firstNumber + secondNumber;
            System.out.println(result);
            
        }
        if (minus == 0){
            result = firstNumber - secondNumber;
            System.out.println(result);
            
        }
        if (multiply == 0){
            result = firstNumber * secondNumber;
            System.out.println(result);
            
        }
        if (division == 0){
            if (secondNumber == 0){
                System.out.println("На 0 делить нельзя");
                System.exit(0);
            }
            result = firstNumber / secondNumber;
            System.out.println(result);
            
        }
    }
}