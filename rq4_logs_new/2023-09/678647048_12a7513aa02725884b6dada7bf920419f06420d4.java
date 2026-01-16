package net.okuri.qol.qolCraft.calcuration;

import org.bukkit.Bukkit;

import java.util.ArrayList;

public class CirculeDistribution implements Calcuration{

    // variable: 位相。cycleで一周する
    // cycle: 周期。
    // distributionNum: 分割数
    // correction: 補正値。分割数と同じ長さでなければならない。答えに足される。
    // これを使用すると、1つの変数を distributionNum の分だけ分割する。補正値を考えない総和は常に1になる。


    private double variable;
    private double cycle;
    private int distributionNum;
    private ArrayList<Double> answer = new ArrayList<>();
    private ArrayList<Double> correction = new ArrayList<>();

    @Override
    public void setVariable(double... variables) {
        // 変数が3個じゃない場合はエラーを出す
        if (variables.length != 3) {
            throw new IllegalArgumentException("Variables are not long enough.");
        }
        this.variable = variables[0];
        this.cycle = variables[1];
        this.distributionNum = (int) variables[2];
    }
    @Override
    public void calcuration(){
        answer = new ArrayList<>();
        for (int i = 0; i < distributionNum; i++) {
            answer.add((1.0/distributionNum)*(Math.sin(2*Math.PI*(((double)i/distributionNum)+(variable/cycle))) + 1.0) + correction.get(i));
        }
    }
    @Override
    public ArrayList<Double> getAns(){
        return this.answer;
    }
    public void setVariable(double variable) {
        this.variable = variable;
    }
    public void setCycle(double cycle) {
        this.cycle = cycle;
    }
    public void setDistributionNum(int distributionNum) {
        this.distributionNum = distributionNum;
    }
    public double getVariable() {
        return this.variable;
    }
    public double getCycle() {
        return this.cycle;
    }
    public int getDistributionNum() {
        return this.distributionNum;
    }
    public ArrayList<Double> getCorrection() {
        return this.correction;
    }
    public void setCorrection(ArrayList<Double> correction) {
        //もし correctionの長さがdistributionNumより短い場合はエラーを出す
        if (correction.size() < distributionNum) {
            throw new IllegalArgumentException("Correction is not long enough.");
        }
        this.correction = correction;
    }
}