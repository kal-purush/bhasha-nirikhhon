package com.example.calculator3;

import com.example.common.exception.BadInputException;
import com.example.common.exception.InvalidOperatorException;

import java.util.regex.Pattern;

public class Parser {
    private static final String OPERATION_REG = "[+\\-*/]";
    private static final String NUMBER_REG = "^[0-9]*$";

    private String firstInput;
    private String secondInput;
    private String operator;

    private final ArithmeticCalculator arithmeticCalculator = new ArithmeticCalculator();

    public ArithmeticCalculator getArithmeticCalculator() {
        return arithmeticCalculator;
    }

    public void validateInputNumbers(String firstInput, String secondInput) throws BadInputException {
        if (!Pattern.matches(NUMBER_REG, firstInput) || !Pattern.matches(NUMBER_REG, secondInput)) {
            throw new BadInputException("숫자타입");
        }
        setFirstInput(firstInput);
        setSecondInput(secondInput);
    }

    public void validateOperator(String operator) throws InvalidOperatorException{
        if (!Pattern.matches(OPERATION_REG, operator)) {
            throw new InvalidOperatorException();
        }
        setOperator(operator);
    }

    public Double executeCalculator() {
        // 여기서 String 타입의 피연산자들을 실수형이나 정수형으로 변환하여 자유롭게 외부에서 타입을 지정할 수 있음
        Double firstNumber = Double.parseDouble(firstInput);
        Double secondNumber = Double.parseDouble(secondInput);
        return arithmeticCalculator.calculate(firstNumber, secondNumber, operator);
    }

    public void setFirstInput(String firstInput) {
        this.firstInput = firstInput;
    }

    public void setSecondInput(String secondInput) {
        this.secondInput = secondInput;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

}