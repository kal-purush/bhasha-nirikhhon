package com.example.calculator2;

import java.util.ArrayList;

public class Calculator {
    ArrayList<Integer> results = new ArrayList<Integer>();

    public int calculate(int number1, int number2, char operator){
        int answer = 0;

        //연산 수행
        if (operator == '+') {
            answer = number1 + number2;
        } else if (operator == '-') {
            answer = number1 - number2;
        } else if (operator == '*') {
            answer = number1 * number2;
        } else if (operator == '/') {
            answer = number1 / number2;
        }

        results.add(answer);
        return answer;
    }
}