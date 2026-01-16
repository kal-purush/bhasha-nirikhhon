package ru.nsu.chaiko;

import java.lang.Math;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Stack;

/**
 * main class of the task.
 */
public class Calculator {
    private final Deque<String> inputData = new LinkedList<>();
    private final Stack<String> stack = new Stack<>();
    private String inputString;
    private boolean debug = false;

    /**
     * firs classes constructors.
     */
    public Calculator(boolean debug) throws NoInputException {
        if (debug) {
            throw new NoInputException("no input expression");
        }
    }

    /**
     * second classes constructor.
     */
    public Calculator(boolean debug, String inputString) throws NoInputException {
        this.debug = debug;

        if (debug) {
            if (inputString.isEmpty()) {
                throw new NoInputException("missed input expression");
            }
            this.inputString = inputString;
        } else {
            this.inputString = "";
        }
    }

    /**
     * getting input.
     */
    private void readData() {
        Scanner scanner;

        if (this.debug) {
            scanner = new Scanner(inputString);
        } else {
            scanner = new Scanner(System.in);
        }

        while (scanner.hasNext()) {
            this.inputData.add(scanner.next());
        }
    }

    /**
     * converts any format number to complex.
     */
    private double[] convertToComplex(String value) {
        double[] res = new double[2];

        if (value.charAt(value.length() - 1) == '`') {
            value = value.replace("`", "");
            res[0] = Double.parseDouble(value) * Math.PI / 180;
            return res;
        }

        if (value.charAt(0) == '(') {
            value = value.replace("(", "");
            value = value.replace(")", "");
            value = value.replace(",", " ");
            String[] numbers = value.split(" ");

            res[0] = Double.parseDouble(numbers[0]);
            res[1] = Double.parseDouble(numbers[1]);

            return res;
        }

        res[0] = Double.parseDouble(value);
        return res;
    }

    /**
     * wrapped arrays representation to strings.
     */
    private String wrapToComplex(double[] elem) {
        StringBuilder builder = new StringBuilder("(");
        builder.append(String.valueOf(elem[0])).append(",");
        builder.append(String.valueOf(elem[1])).append(")");

        return builder.toString();
    }

    /**
     * processing input expression.
     */
    String calculateAnswer() throws NoNumberException, IncorrectInputFormatException {
        readData();

        while (this.inputData.size() != 0) {
            String elem = this.inputData.removeLast();

            try {
                // ` - means degree
                if (elem.charAt(elem.length() - 1) == '`') {
                    this.stack.push(elem);
                    continue;
                }

                if (elem.charAt(0) == '(' && elem.charAt(elem.length() - 1) == ')') {
                    this.stack.push(elem);
                    continue;
                }

                Double.parseDouble(elem);
                this.stack.push(elem);

            } catch (NumberFormatException ignored) {
                if (this.stack.isEmpty()) {
                    throw new NoNumberException("no number to calculate with");
                }

                double[] result = new double[2];
                String[] values = new String[2];
                values[0] = this.stack.pop();
                double[][] complexValues = new double[2][2];

                switch (elem) {
                    case "+":
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }

                        values[1] = this.stack.pop();
                        complexValues[0] = convertToComplex(values[0]);
                        complexValues[1] = convertToComplex(values[1]);

                        complexValues[0][0] = complexValues[0][0] + complexValues[1][0];
                        complexValues[0][1] = complexValues[0][1] + complexValues[1][1];

                        this.stack.push(wrapToComplex(complexValues[0]));

                        break;

                    case "-":
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }

                        values[1] = this.stack.pop();
                        complexValues[0] = convertToComplex(values[0]);
                        complexValues[1] = convertToComplex(values[1]);

                        complexValues[0][0] = complexValues[0][0] - complexValues[1][0];
                        complexValues[0][1] = complexValues[0][1] - complexValues[1][1];

                        this.stack.push(wrapToComplex(complexValues[0]));

                        break;

                    case "*":
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }

                        values[1] = this.stack.pop();
                        complexValues[0] = convertToComplex(values[0]);
                        complexValues[1] = convertToComplex(values[1]);

                        result[0] = complexValues[0][0] * complexValues[1][0]
                                -
                                complexValues[0][1] * complexValues[1][1];
                        result[1] = complexValues[0][0] * complexValues[1][1]
                                +
                                complexValues[0][1] * complexValues[1][0];

                        this.stack.push(wrapToComplex(result));

                        break;

                    case "/":
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }

                        values[1] = this.stack.pop();
                        complexValues[0] = convertToComplex(values[0]);
                        complexValues[1] = convertToComplex(values[1]);

                        result[0] = (complexValues[0][0] * complexValues[1][0]
                                +
                                complexValues[0][1] * complexValues[1][1])
                                /
                                (Math.pow(complexValues[1][0], 2) + Math.pow(complexValues[1][1], 2));
                        result[1] = (complexValues[1][0] * complexValues[0][1]
                                -
                                complexValues[0][0] * complexValues[1][1])
                                /
                                (Math.pow(complexValues[1][0], 2) + Math.pow(complexValues[1][1], 2));

                        this.stack.push(wrapToComplex(result));

                        break;

                    case "sin":
                        complexValues[0] = convertToComplex(values[0]);

                        if (complexValues[0][1] != 0) {
                            throw new IncorrectInputFormatException(
                                    "can't apply" + elem + "func to complex value");
                        }

                        result[0] = Math.sin(complexValues[0][0]);

                        this.stack.push(wrapToComplex(result));

                        break;

                    case "cos":
                        complexValues[0] = convertToComplex(values[0]);

                        if (complexValues[0][1] != 0) {
                            throw new IncorrectInputFormatException(
                                    "can't apply" + elem + "func to complex value");
                        }

                        result[0] = Math.cos(complexValues[0][0]);

                        this.stack.push(wrapToComplex(result));

                        break;

                    case "sqrt":
                        complexValues[0] = convertToComplex(values[0]);

                        if (complexValues[0][1] != 0) {
                            throw new IncorrectInputFormatException(
                                    "can't apply" + elem + "func to complex value");
                        }

                        result[0] = Math.sqrt(complexValues[0][0]);

                        this.stack.push(wrapToComplex(result));

                        break;

                    case "pow":
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }

                        values[1] = this.stack.pop();
                        complexValues[0] = convertToComplex(values[0]);
                        complexValues[1] = convertToComplex(values[1]);

                        if (complexValues[1][1] != 0) {
                            throw new IncorrectInputFormatException(
                                    "second argument of pow must be rational");
                        }

                        result[0] = complexValues[0][0];
                        result[1] = complexValues[0][1];

                        for (int i = 0; i < complexValues[1][0] - 1; i++) {
                            double subtotal1 = complexValues[0][0] * result[0] -
                                    complexValues[0][1] * result[1];
                            double subtotal2 = complexValues[0][0] * result[1] +
                                    complexValues[0][1] * result[0];

                            result[0] = subtotal1;
                            result[1] = subtotal2;
                        }

                        this.stack.push(wrapToComplex(result));

                        break;

                    case "log":
                        complexValues[0] = convertToComplex(values[0]);

                        if (complexValues[0][1] != 0) {
                            throw new IncorrectInputFormatException(
                                    "can't apply" + elem + "func to complex value");
                        }

                        result[0] = Math.log(complexValues[0][0]);

                        this.stack.push(wrapToComplex(result));

                        break;

                    default:
                        throw new IllegalArgumentException("unsupported function");
                }
            }
        }
        return this.stack.pop();
    }
}