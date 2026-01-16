package com.datamasking.helperClasses;

import java.util.ArrayList;

public class AlgorithmItem {
    String algo;
    Integer k;
    Integer l;
    Integer t;
    String pattern;
    ArrayList<String> sensitive_attributes;
    ArrayList<String> xPaths;
    ArrayList<Integer> rangeMax;

    public ArrayList<Integer> getRangeMax() {
        return rangeMax;
    }

    public void setRangeMax(ArrayList<Integer> rangeMax) {
        this.rangeMax = rangeMax;
    }

    public AlgorithmItem(String algo, Integer k, Integer l, Integer t, String pattern, ArrayList<String> sensitive_attributes, ArrayList<String> xPaths, ArrayList<Integer> rangeMax) {
        this.algo = algo;
        this.k = k;
        this.l = l;
        this.t = t;
        this.pattern = pattern;
        this.sensitive_attributes = sensitive_attributes;
        this.xPaths = xPaths;
        this.rangeMax = rangeMax;
    }

    public String getAlgo() {
        return algo;
    }

    public void setAlgo(String algo) {
        this.algo = algo;
    }

    public Integer getK() {
        return k;
    }

    public void setK(Integer k) {
        this.k = k;
    }

    public Integer getL() {
        return l;
    }

    public void setL(Integer l) {
        this.l = l;
    }

    public Integer getT() {
        return t;
    }

    public void setT(Integer t) {
        this.t = t;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public ArrayList<String> getSensitive_attributes() {
        return sensitive_attributes;
    }

    public void setSensitive_attributes(ArrayList<String> sensitive_attributes) {
        this.sensitive_attributes = sensitive_attributes;
    }

    public ArrayList<String> getxPaths() {
        return xPaths;
    }

    public void setxPaths(ArrayList<String> xPaths) {
        this.xPaths = xPaths;
    }
}