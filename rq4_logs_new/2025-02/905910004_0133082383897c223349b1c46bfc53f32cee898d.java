package lesson_16_Functional_Programming.livecoding;

public class Calculator {

    int result(TestInterface testInterface, int a1, int a2) {
        return testInterface.calculate(a1,a2);
    }

    public static void main(String[] args) {
        Calculator calculator = new Calculator();
        System.out.println(calculator.result(Integer::sum, 5,46));
        System.out.println(calculator.result((a1,a2) -> a1 - a2, 5,45));
        System.out.println(calculator.result((a1,a2) -> a1 / a2, 5,45));
        System.out.println(calculator.result((a1,a2) -> a1 * a2, 5,45));
    }
}