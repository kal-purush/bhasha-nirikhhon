package ru.sberbank.edu;

public class FinancialFields {
    private Integer years;
    private Double sum;
    private Double percentage;

    public FinancialFields(Integer years, Double sum, Double percentage) {
        this.years = years;
        this.sum = sum;
        this.percentage = percentage;
    }

    public Integer getYears() {
        return years;
    }

    public void setYears(Integer years) {
        this.years = years;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Double getPercentage() {
        return percentage;
    }

    public void setPercentage(Double percentage) {
        this.percentage = percentage;
    }

    @Override
    public String toString() {
        return "FinancialFields{" +
                "years=" + years +
                ", sum=" + sum +
                ", percentage=" + percentage +
                '}';
    }
}