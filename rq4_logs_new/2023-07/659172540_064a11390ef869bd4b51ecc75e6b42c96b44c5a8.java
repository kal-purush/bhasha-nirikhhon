package stepikCourse.Java.wrapperClass;

import java.util.ArrayList;

public class Test1Equals {
    public static void main(String[] args) {
        Car c1 = new Car("red", "V4");
        Car c2 = new Car("red", "V4");
        Car c3 = new Car("black", "V8");
        System.out.println(c1.equals(c2));//true так как с1 равен с2


        ArrayList <Car> list = new ArrayList<>();
        list.add(c1);
        list.add(c2);
        list.add(c3);
        Car c4 = new Car("black", "V8");
        System.out.println(list.contains(c4));

    }
}

class Car {

    String color;
    String engine;

    Car(String color, String engine) { //создали конструктор
        this.color = color;
        this.engine = engine;
    }

    public boolean equals(Car c) {

            return color.equals(c.color) && engine.equals(c.engine);
        }
    }



