package Java_Home_Work.HW_6;

import java.util.Random;

public abstract class Animal {
    protected String type;
    protected String name;
    protected int run;
    protected abstract String swim();
    protected Random random = new Random();
    public static int animal_count = 0;

    public Animal(String type, String name, int run) {
        this.type = type;
        this.name = name;
        this.run = run;
        ++ animal_count;
    }
    public String race(){
        int distance = random.nextInt(1, 600);
        if (run > distance) {
            return (type + " " + name +  " was able to run " + distance + " meters. Fine!");
        } else {
            return (type + " " + name + " could not run " + distance + " meters. Do not be sad!");
        }
    }
    public void lets_run(){
        System.out.println(race());
        }
        public void lets_swim(){
        System.out.println(swim());
        }
    }