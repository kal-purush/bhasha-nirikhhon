package net.okuri.qol.qolCraft.calcuration;

import java.util.ArrayList;

public class StatisticalCalcuration implements Calcuration{
    private ArrayList<Double> vers = new ArrayList<>();
    private double mean;
    private double variance;
    private double standardDeviation;
    private double median;
    private double mode;
    private double max;
    private double min;
    private double range;
    private double sum;
    private ArrayList<Double> ans = new ArrayList<>();


    @Override
    public void setVariable(double... variables) {
        this.vers.clear();
        for (double variable : variables) {
            this.vers.add(variable);
        }
    }

    @Override
    public void calcuration() {
        this.mean = this.vers.stream().mapToDouble(Double::doubleValue).sum() / this.vers.size();
        this.variance = this.vers.stream().mapToDouble(v -> Math.pow(v - this.mean, 2)).sum() / this.vers.size();
        this.standardDeviation = Math.sqrt(this.variance);
        this.median = this.vers.get(this.vers.size() / 2);
        this.max = this.vers.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
        this.min = this.vers.stream().mapToDouble(Double::doubleValue).min().getAsDouble();
        this.range = this.max - this.min;
        this.sum = this.vers.stream().mapToDouble(Double::doubleValue).sum();
        this.ans = new ArrayList<>();
        this.ans.add(this.mean);
        this.ans.add(this.variance);
        this.ans.add(this.standardDeviation);
        this.ans.add(this.median);
        this.ans.add(this.max);
        this.ans.add(this.min);
        this.ans.add(this.range);
        this.ans.add(this.sum);

    }

    @Override
    public ArrayList<Double> getAns() {
        return this.ans;
    }
    public double getMean() {
        return this.mean;
    }
    public double getVariance() {
        return this.variance;
    }
    public double getStandardDeviation() {
        return this.standardDeviation;
    }
    public double getMedian() {
        return this.median;
    }
    public double getMax() {
        return this.max;
    }
    public double getMin() {
        return this.min;
    }
    public double getRange() {
        return this.range;
    }
    public double getSum() {
        return this.sum;
    }
}