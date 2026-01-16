import javafx.css.Match;

import java.util.Scanner;

public class MatchApp {
    public static void main(String[] args){
        Scanner in = new Scanner(System.in);

        double FirstNumber;
        double SecondNumber;
        String MatchOperation;

        System.out.print("Введите выражение для вычисления (например: 8 / 4):");

        FirstNumber = in.nextDouble();
        MatchOperation = in.next();
        SecondNumber = in.nextDouble();

        switch(MatchOperation) {
            case "+":
                System.out.println("Результат вычисления:" + (FirstNumber + SecondNumber));
                break;
            case "-":
                System.out.println("Результат вычисления:" + (FirstNumber - SecondNumber));
                break;
            case "*":
                System.out.println("Результат вычисления:" + (FirstNumber * SecondNumber));
                break;
            case "/":
                System.out.println("Результат вычисления:" + (FirstNumber / SecondNumber));
                break;
            case "%":
                System.out.println("Результат вычисления:" + (FirstNumber % SecondNumber));
                break;

        }
    }
}