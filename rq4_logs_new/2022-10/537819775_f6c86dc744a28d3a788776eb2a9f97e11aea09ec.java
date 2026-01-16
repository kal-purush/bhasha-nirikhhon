package lecture4;

import static java.lang.Math.*;

public class App {
    public static void main(String[] args) {
        Human human = new Human();
        human.name = "Vasya";
        System.out.println("human.name = " + human.name);
        System.out.println("human.age = " + human.age);
        human.sayHello();


        Human human1 = new Human("Kolya", 25);
        System.out.println(human1);

        Human human2 = new Human("Kolya", 25);
        System.out.println(human1.equals(human2));
        human2.born();
        System.out.println(human.type);
        human2.type = "Sapiens";
        System.out.println(human.type);
        System.out.println(Human.type);
        System.out.println(sum(1,2));

        Human human3 = new Human("Sveta");
        System.out.println(human3);

        System.out.println(PI);
        System.out.println(E);
    }


    public static double sum (short s1, short s2){
        System.out.println("sum (short s1, short s2");
        return s1 + s2;
    }

    public static double sum (double s1, double s2){
        System.out.println("sum (double s1, double s2");
        return s1 + s2;
    }

    public static double sum (long s1, long s2){
        System.out.println("sum (long s1, long s2");
        return s1 + s2;
    }
}