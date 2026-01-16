package com.example.calculator3;

import java.util.ArrayList;

public class ArithmeticCalculator {

    private OperatorType addOperator = OperatorType.ADD;
    private OperatorType subtractOperator = OperatorType.SUBTRACT;
    private OperatorType divideOperator = OperatorType.DIVIDE;
    private OperatorType multiplyOperator = OperatorType.MULTIPLY;

    private ArrayList<Double> results = new ArrayList<>();

    public <T extends Number> Double calculate(T number1, T number2, char operator){
        double answer = 0.0;

        //연산 수행
        if (operator == addOperator.getOperator()) {
            answer = number1.doubleValue() + number2.doubleValue();
        } else if (operator == subtractOperator.getOperator()) {
            answer = number1.doubleValue() - number2.doubleValue();
        } else if (operator == multiplyOperator.getOperator()) {
            answer = number1.doubleValue() * number2.doubleValue();
        } else if (operator == divideOperator.getOperator()) {
            answer = number1.doubleValue() / number2.doubleValue();
        }

        results.add(answer);
        return answer;
    }

    public String getAllResults(){
        return results.toString();
    }

    public Double getResults(int index){
        return results.get(index);
    }

    public void setResult(int index, double value){
        results.set(index, value);
    }

    public void deleteFirstResult(){
        results.remove(0);
    }

}