package ru.otus.java.basic.safarov.homework05;

abstract public class Animal {
    protected String name;
    protected int speedRun;
    protected int speedSwim;
    protected int stamina;
    protected int staminaRun;
    protected int staminaSwim;
    protected String typeAnimal;


    public Animal(String name, int speedRun, int stamina) {
        this.name = name;
        this.speedRun = speedRun;
        this.stamina = stamina;
        staminaRun = 1;
    }

    public String getName() {
        return name;
    }

    public int getSpeedRun() {
        return speedRun;
    }

    public int getSpeedSwim() {
        return speedSwim;
    }

    public int getStamina() {
        return stamina;
    }

    protected int move(int distance, boolean typeMove) {
        int speed;
        int staminaMove;
        if (typeMove) {
            speed = speedRun;
            staminaMove = staminaRun;
        } else {
            speed = speedSwim;
            staminaMove = staminaSwim;
        }
        int time = distance / speed;
        int staminaExpendable = distance * staminaMove;
        stamina -= staminaExpendable;
        return stamina >= 0 ? time : -1;
    }

    public int run(int distance) {
        int time = move(distance, true);
        if (time == -1) {
            System.out.println(typeAnimal + " " + name + " должен был пробежать "
                    + distance + " м, но выносливости не хватило. " +
                    "Состояние " + name + " усталость.");
        } else {
            System.out.println(typeAnimal + " " + name + " пробежал " + distance + " м за " + time + " c.");
        }
        return time;
    }

    public int swim(int distance) {
        if (speedSwim == -1){
            System.out.println(typeAnimal + " не умеет плавать.");
            return -1;
        }
        int time = move(distance, false);
        if (time == -1) {
            System.out.println(typeAnimal + " " + name + " должен был проплыть "
                    + distance + " м, но выносливости не хватило. " +
                    "Состояние " + name + " усталость.");
        } else {
            System.out.println(typeAnimal + " " + name + " проплыл " + distance + " м за " + time + " c.");
        }
        return time;
    }

    public void info() {
        System.out.println(typeAnimal + " " + name + " состояние "
                + ((stamina < 0) ? "усталость" : ("выносливость " + stamina)));
    }
}