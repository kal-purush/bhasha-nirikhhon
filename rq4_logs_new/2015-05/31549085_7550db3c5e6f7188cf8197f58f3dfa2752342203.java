package lesson150525.concurrency;

import java.util.LinkedList;
import java.util.List;

import utils.Utils;

public class AwaitCondition {

    static class Sprinter implements Runnable {

        volatile boolean start = false; // visibility

        @Override
        public void run() {
            int count = 0;
            System.out.println("created " + this);
            while (!start){
                count++;
//                if (count % 1000 == 0) {
//                    Thread.yield();
//                }
            }
            System.out.println(this + " started after " + count + " cycles");
        }
        
        public void start() {
            start = true;
        }
    }
    
    
    public static void main(String[] args) {
        System.out.println("begin");
        List<Sprinter> list = new LinkedList<>();
        
        list.add(startSprinter("First"));
        list.add(startSprinter("Second"));
        list.add(startSprinter("Thrid"));
        
        
        Utils.pause(3000);
        System.out.println("Ready!");
        Utils.pause(1000);
        System.out.println("Stady!");
        Utils.pause(1000);
        System.out.println("Go!");
        
        for (Sprinter sp : list) {
            sp.start();
        }
        Sprinter a = new Sprinter();
    }


    private static Sprinter startSprinter(final String name) {
        Utils.pause(1000);
        System.out.println(name);
        Sprinter sprinter = new Sprinter();
        new Thread(sprinter).start();
        return sprinter;
    }
}