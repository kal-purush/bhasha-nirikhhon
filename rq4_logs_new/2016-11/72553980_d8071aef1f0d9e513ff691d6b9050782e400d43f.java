package com.gmail.grzegorz2047.violationlogger;

/**
 * Created by grzeg on 02.11.2016.
 */
public class Violation {
    private String player;
    private String violationType;
    private int power;
    private String arena;

    public Violation(String message, String arena) {
        this.arena = arena;
        String msg = message.replaceAll("&[a-z]", "").replaceAll("§[a-z]", "");
        System.out.println(msg);
        String[] args = msg.split(":");
        if (args.length == 4) {
            player = args[1];
            violationType = args[2];
            if (args[3].length() <= 5) {
                power = Integer.parseInt(args[3]);
            } else {
                if (args[3].equals("9223372036854775807")) {
                    power = 5000;
                }
            }
        }
    }

    public String getPlayer() {
        return player;
    }

    public String getViolationType() {
        return violationType;
    }

    public int getPower() {
        return power;
    }

    public String getArena() {
        return arena;
    }
}