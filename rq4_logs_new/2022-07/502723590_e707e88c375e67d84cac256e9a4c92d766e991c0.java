package File_work.RTTI;

import java.lang.reflect.Field;
import java.util.Scanner;

public class Task_1 {
    public static void main(String[] args) {
        System.out.println("Print class name - ");
        String className = new Scanner(System.in).next();
        try {
            Class<?> c = Class.forName(className);
            for (Field f : c.getDeclaredFields()) {
                System.out.println("имя - " + f.getName() + ", тип - " + f.getType());
            }
        }
        catch (ClassNotFoundException e) {
            System.err.println("Error class name!");
        }
    }
}