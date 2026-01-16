
public class Calculator {
    public double calculate(int firstNumber, int secondNumber, String symbol) {
        switch (symbol) {
            case "+" -> {
                return firstNumber + secondNumber;
            }
            case "-" -> {
                return firstNumber - secondNumber;
            }
            case "/" -> {
                return (double) firstNumber / secondNumber;
            }
            case "*" -> {
                return firstNumber * secondNumber;
            }
            default -> {
                System.out.println("옯바른 사칙연산 기호를 선택해주세요.");
                return 0;
            }
        }
    }
}