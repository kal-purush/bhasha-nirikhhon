import java.util.ArrayList;

public class CircleCalculator extends Calculator {
    private ArrayList<Double> circleArea;
    private final static double PI = 3.14;

    public CircleCalculator(ArrayList<Double> circleArea) {
        this.circleArea = circleArea;
    }

    public void calculate(int radius) {
        double result = radius * radius * PI;
        this.circleArea.add(result);
    }

    public void removeResult() {
        this.circleArea.removeFirst();
    }

    public void inquiryResults() {
        String str = this.circleArea.toString();
        System.out.println(str);
    }
}