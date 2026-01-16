package org.example;

public class AccessModifiers {
    public static void main(String[] args) {
        Car car = new Honda();
        car.model("civic");
        car.make();
    }

}
abstract class Car{
    //abstract void model();
    public void make(){
        System.out.println("this is year pf car making is 2024 December ");
    }

    abstract void model(String modelName);
}
class Honda extends Car{

    @Override
    void model(String modelName) {
        System.out.println("model name is : "+ modelName);
    }
}