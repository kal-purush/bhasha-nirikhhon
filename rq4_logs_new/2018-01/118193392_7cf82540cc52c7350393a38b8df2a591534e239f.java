package ru;

import java.util.regex.Pattern;
import java.util.*;
import java.text.*;


public class MyCalculator {
    public String evaluate(String statement) {
        LinkedList<Double> value;
        try {
            String newStatement = statement.replaceAll(" ", "");

            value = new LinkedList<Double>();

            LinkedList<Character> Operators = new LinkedList<Character>();


            for (int i = 0; i < newStatement.length(); i++) {

                char symbol = newStatement.charAt(i);

                if (symbol == '(') {

                    Operators.add('(');

                } else if (symbol == ')') {

                    while (Operators.getLast() != '(') {

                        Operation(value, Operators.removeLast());

                    }

                    Operators.removeLast();

                } else if (isOperator(symbol)) {

                    while (!Operators.isEmpty() && priority(Operators.getLast()) >= priority(symbol)) {

                        Operation(value, Operators.removeLast());

                    }

                    Operators.add(symbol);

                } else if (Character.isDigit(symbol)) {

                    String number = "";

                    while (i < newStatement.length() && (newStatement.charAt(i) == '.' || Character.isDigit(newStatement.charAt(i))
                            || newStatement.charAt(i) == ',')) {

                        if (newStatement.charAt(i) == ',') {

                            return null;

                        }

                        number += newStatement.charAt(i++);

                    }
                    --i;

                    try {

                        value.add(Double.parseDouble(number));

                    } catch (NumberFormatException e) {

                        return null;

                    }
                }
            }
            while (!Operators.isEmpty()) {

                Operation(value, Operators.removeLast());

            }
        } catch (Exception e) {

            return null;

        }
        try {

            NumberFormat numberFormatter = NumberFormat.getNumberInstance(Locale.US);
            numberFormatter.setMaximumFractionDigits(4);
            numberFormatter.setMinimumFractionDigits(0);

            return numberFormatter.format(value.get(0));

        } catch (Exception e) {
            return null;
        }

    }

    public boolean isOperator(char ch) {

        return ch == '+' || ch == '-' || ch == '*' || ch == '/';

    }

    public int priority(char operator) {

        if (operator == '*' || operator == '/') {
            return 1;
        } else if (operator == '+' || operator == '-') {
            return 0;
        } else {
            return -1;
        }
    }

    public void Operation(LinkedList<Double> st, char operator) throws Exception {

        double one = st.removeLast();
        double two = st.removeLast();


        switch (operator) {
            case '+':
                st.add(two + one);
                break;
            case '-':
                st.add(two - one);
                break;
            case '*':
                st.add(two * one);
                break;
            case '/':
                if (one == 0) {
                    throw new ArithmeticException();
                }
                st.add(two / one);
                break;
            default:
                System.out.println("Нет такого оператора");
        }
    }
}