package com.example.spacetrader.entity;

public class Player {
    private int skillPilot, skillFighter, skillTrader, skillEngineer;
    private String name;
    private Difficulty difficulty;

    public Player(String name, Difficulty difficulty, int skillPilot, int skillFighter, int skillTrader, int skillEngineer) {
        this.name = name;
        this.difficulty = difficulty;
        this.skillPilot = skillPilot;
        this.skillFighter = skillFighter;
        this.skillTrader = skillTrader;
        this.skillEngineer = skillEngineer;
    }
}