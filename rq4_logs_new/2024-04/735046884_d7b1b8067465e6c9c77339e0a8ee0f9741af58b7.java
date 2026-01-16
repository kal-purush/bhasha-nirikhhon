package dev.andrylat.vomelianchuk.finances.mortgagecalculator.dto;

import java.util.List;

public class MortgageCalculatorDTO {
    private double totalPrice;
    private double downPayment;
    private double interestRate;
    private double numberOfYears;

    public MortgageCalculatorDTO(List<Double> dataMortgage) {
        this.totalPrice = dataMortgage.get(0);
        this.downPayment = dataMortgage.get(1);
        this.interestRate = dataMortgage.get(2);
        this.numberOfYears = dataMortgage.get(3);
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public double getDownPayment() {
        return downPayment;
    }

    public void setDownPayment(double downPayment) {
        this.downPayment = downPayment;
    }

    public double getInterestRate() {
        return interestRate;
    }

    public void setInterestRate(double interestRate) {
        this.interestRate = interestRate;
    }

    public double getNumberOfYears() {
        return numberOfYears;
    }

    public void setNumberOfYears(double numberOfYears) {
        this.numberOfYears = numberOfYears;
    }

}