import java.util.ArrayList;

public class ArithmeticCalculator extends Calculator{
    private ArrayList<Double> arrayList;

    public ArithmeticCalculator(ArrayList<Double> arrayList) {
        this.arrayList = arrayList;
    }

    public void calculate(int firstNumber, int secondNumber, char operator) {
        double result;
        switch (operator) {
            case '+' -> {
                result = firstNumber + secondNumber;
                this.arrayList.add(result);
            }
            case '-' -> {
                result = firstNumber - secondNumber;
                this.arrayList.add(result);
            }
            case '/' -> {
                result = (double) firstNumber / secondNumber;
                this.arrayList.add(result);
            }
            case '*' -> {
                result = firstNumber * secondNumber;
                this.arrayList.add(result);
            }
            default -> {
                System.out.println("옯바른 사칙연산 기호를 사용해주세요.");
            }
        }
    }


    @Override
    public void removeResult() {
        arrayList.removeFirst();
    }

    @Override
    public void inquiryResults() {
        String str = arrayList.toString();
        System.out.println(str);
    }
}