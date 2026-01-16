package com.example.shane.getmeup;

public class Alarm {
    private String name, days, time;
    private Boolean shake, walk, puzzle, speak;

    public Alarm(String n, String d, String t, Boolean s, Boolean w, Boolean p, Boolean sp)
    {
        setName(n);
        setDays(d);
        setTime(t);
        setShake(s);
        setWalk(w);
        setPuzzle(p);
        setSpeak(sp);
    }
    public String getName() {
        return name;
    }
    public void setName(String n) {
        name = n;
    }
    public String getDays() {
        return days;
    }
    public void setDays(String d) {
        days = d;
    }
    public String getTime() {
        return time;
    }
    public void setTime(String t) {
       time = t;
    }
    public Boolean getShake() {
        return shake;
    }
    public void setShake(Boolean s) {
        shake = s;
    }
    public Boolean getWalk() {
        return walk;
    }
    public void setWalk(Boolean w) {
        walk = w;
    }
    public Boolean getPuzzle() {
        return puzzle;
    }
    public void setPuzzle(Boolean p) {
        puzzle = p;
    }
    public Boolean getSpeak() {
        return speak;
    }
    public void setSpeak(Boolean sp) {
        speak = sp;
    }

}