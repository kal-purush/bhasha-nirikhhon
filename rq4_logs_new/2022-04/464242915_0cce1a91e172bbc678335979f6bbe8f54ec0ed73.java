package com.datamasking.helperClasses;

public class Pair {
    private String first;
    private String second;

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Pair(String first, String second) {
        this.first = first;
        this.second = second;
    }
}