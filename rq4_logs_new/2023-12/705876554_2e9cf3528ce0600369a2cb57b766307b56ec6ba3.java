package ru.edu;

import static java.lang.Math.round;

public class Calculation {
    private FinancialFields financialFields = null;
    private double profit;

    public double getProfit() {
        return profit;
    }

    public Calculation(FinancialFields financialFields) {
        this.financialFields = financialFields;
        profit = this.financialFields.getSum() * Math.pow(1 + this.financialFields.getPercentage()/100, this.financialFields.getYears());
        profit = profit * 100;
        profit = Math.round(profit);
        profit = profit / 100;
    }

}