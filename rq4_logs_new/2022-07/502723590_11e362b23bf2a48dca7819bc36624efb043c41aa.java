package File_work.RTTI;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import java.util.Scanner;

public class task_5 {
    public static void main(String[] args) {
        System.out.println("print method");
        String method = new Scanner(System.in).next();
        try {
            Class<?> cls = Class.forName("File_work.ArrayX");
            Method m = cls.getMethod(method);
            for (Annotation ann : m.getAnnotations()) {
                System.out.println(ann);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}