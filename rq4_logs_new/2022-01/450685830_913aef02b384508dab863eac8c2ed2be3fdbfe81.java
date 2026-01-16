package com.example.calculatorgb;

import java.util.Locale;

public class Calculator {

    protected Double valueDbl = 0d;
    protected String operation = null;

    public Calculator() {
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public void setValueDbl(Double valueDbl) {
        this.valueDbl = valueDbl;
    }

    public String buttonPush(String num, String value) {
        if (value.equals("0") && !num.equals(".")) {
            return num;
        } else {
            return String.format(Locale.getDefault(), "%s", value + num);
        }
    }

    public String operationPush(String operation, String value) {
        this.operation = operation;
        this.valueDbl = Double.parseDouble(value);
        return String.format(Locale.getDefault(), "%s", value + operation);
    }

    public String resultPush(String value) {
        switch (this.operation) {
            case "+":
                this.valueDbl = this.valueDbl + Double.parseDouble(value);
                break;

            case "-":
                this.valueDbl = this.valueDbl - Double.parseDouble(value);
                break;

            case "x":
                this.valueDbl = this.valueDbl * Double.parseDouble(value);
                break;

            case "/":
                this.valueDbl = this.valueDbl / Double.parseDouble(value);
                break;
        }
        this.operation = null;
        return String.valueOf(valueDbl);
    }

}