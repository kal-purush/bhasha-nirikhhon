package com.it.academy.mortgage.calculator.dto;

public class CalculateResultsDto {

    private double maxLoan;
    private double totalInterestPaid;
    private double agreementFee;
    private double totalPaymentSum;

    public double getMaxLoan() {
        return maxLoan;
    }

    public void setMaxLoan(double maxLoan) {
        this.maxLoan = maxLoan;
    }

    public double getTotalInterestPaid() {
        return totalInterestPaid;
    }

    public void setTotalInterestPaid(double totalInterestPaid) {
        this.totalInterestPaid = totalInterestPaid;
    }

    public double getAgreementFee() {
        return agreementFee;
    }

    public void setAgreementFee(double agreementFee) {
        this.agreementFee = agreementFee;
    }

    public double getTotalPaymentSum() {
        return totalPaymentSum;
    }

    public void setTotalPaymentSum(double totalPaymentSum) {
        this.totalPaymentSum = totalPaymentSum;
    }
}