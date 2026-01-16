package calculator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class CalCulator {

    private final Set<String> operators = new HashSet<>(Arrays.asList("+", "-", "*", "/"));

    public double calculate(String expr) {

        if (valid(expr)) {
            return parseExpr(expr);
        } else {
            throw new IllegalArgumentException("Expr Error");
        }
    }

    public double partCalculate(double a, double b, String op) {

        double result = 0;

        switch (op) {
            case "+":
                result = a + b;
                break;
            case "-":
                result = a - b;
                break;
            case "*":
                result = a * b;
                break;
            case "/":
                result = a / b;
                break;
            default:
                break;
        }

        return result;
    }

    public boolean valid(String expr) {
        String pattern = "^\\d ([\\+|\\-|\\*|/|] \\d)+$";

        return Pattern.matches(pattern, expr);
    }

    public double parseExpr(String expr) {

        String[] values = expr.split(" ");

        double result = Double.parseDouble(values[0]);
        String op = "";

        for (int i = 1; i < values.length; i++) {
            if (operators.contains(values[i]))
                op = values[i];
            else {
                result = partCalculate(result, Double.parseDouble(values[i]), op);
            }
        }

        return result;
    }

}