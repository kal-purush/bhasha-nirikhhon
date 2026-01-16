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
    private final Stack<Double> stack = new Stack<>();
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
     * processing input expression.
     */
    double calculateAnswer() throws NoNumberException {
        readData();

        while (this.inputData.size() != 0) {
            var elem = this.inputData.removeLast();

            try {

                this.stack.add(Double.parseDouble(elem));

            } catch (NumberFormatException ignored) {
                if (this.stack.isEmpty()) {
                    throw new NoNumberException("no number to calculate with");
                }

                double first = this.stack.pop(), second;
                switch (elem) {
                    case ("+") -> {
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }
                        second = this.stack.pop();
                        this.stack.push(first + second);
                    }
                    case ("-") -> {
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }
                        second = this.stack.pop();
                        this.stack.push(first - second);
                    }
                    case ("*") -> {
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }
                        second = this.stack.pop();
                        this.stack.push(first * second);
                    }
                    case ("/") -> {
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }
                        second = this.stack.pop();
                        this.stack.push(first / second);
                    }
                    case ("sin") -> this.stack.push(Math.sin(first));
                    case ("cos") -> this.stack.push(Math.cos(first));
                    case ("sqrt") -> this.stack.push(Math.sqrt(first));
                    case ("pow") -> {
                        if (this.stack.size() == 0) {
                            throw new NoNumberException("missed second argument");
                        }
                        second = this.stack.pop();
                        this.stack.push(Math.pow(first, second));
                    }
                    case ("log") -> this.stack.push(Math.log(first));
                }
            }
        }
        return this.stack.pop();
    }
}