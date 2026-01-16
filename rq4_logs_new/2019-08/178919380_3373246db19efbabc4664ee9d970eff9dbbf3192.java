package ru.otus.homework.dao;

public class Account {
    @Id
    private long no;
    private String type;
    private double rest;
    private int level;

    public Account() {
    }

    public Account(String type, double rest, int level) {
        this.type = type;
        this.rest = rest;
        this.level = level;
    }

    public Account(long no, String type, double rest, int level) {
        this.no = no;
        this.type = type;
        this.rest = rest;
        this.level = level;
    }

    public String getType() {
        return type;
    }

    public double getRest() {
        return rest;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public String toString() {
        return "Account{" +
                "no=" + no +
                ", type='" + type + '\'' +
                ", rest=" + rest +
                ", level=" + level +
                '}';
    }
}