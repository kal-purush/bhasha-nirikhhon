package com.example.calculator;

public class Processor {
    private int inputNumber1;
    private int inputNumber2;
    private int result;
    private MathOperation lastOperation = MathOperation.NONE;
    private boolean hasOperation;

    public boolean getHasOperation() {
        return hasOperation;
    }

    public int getInputNumber1() {
        return inputNumber1;
    }

    public void setInputNumber1(int value) {
        inputNumber1 = value;
    }

    public void setInputNumber2(int value) {
        inputNumber2 = value;
    }

    public int getResult() {
        return result;
    }

    public void processOperation(MathOperation operation) {
        if (operation == MathOperation.CLEAR) {
            inputNumber1 = 0;
            inputNumber2 = 0;
            result = 0;
            lastOperation = MathOperation.NONE;
            return;
        }

        updateResult(inputNumber1, inputNumber2);

        if (operation == MathOperation.EQUALLY) {
            inputNumber1 = 0;
            inputNumber2 = 0;
            lastOperation = MathOperation.NONE;
        } else {
            lastOperation = operation;
        }
        hasOperation = true;
    }

    public void processNumber() {
        if (hasOperation) {
            hasOperation = false;
        }
    }

    private void updateResult(int number1, int number2) {
        switch (lastOperation) {
            case PLUS:
                result = number1 + number2;
                break;
            case MINUS:
                result = number1 - number2;
                break;
            case MULTIPLICATION:
                result = number1 * number2;
                break;
            case DIVIDE:
                if (number2 == 0) {
                    result = 0;
                    break;
                }
                result = number1 / number2;

                break;
        }
    }
}