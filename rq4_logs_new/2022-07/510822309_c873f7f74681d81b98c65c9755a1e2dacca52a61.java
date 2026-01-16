package ru.sber.oop1.oop1.Task1;
import java.util.Arrays;

public class Phone {
    String number;
    String model;
    double weight;

    void receiveCall(String name) {
        System.out.println("Звонит " + name);
    }

    void receiveCall(String name, String number) {
        System.out.println(("Звонит " + name + " Номер " + number));
    }

    public String getNumber() {
        return number;
    }

    Phone(String number, String model, double weight) {
        this.number = number;
        this.model = model;
        this.weight = weight;
    }

    Phone(String number, String model) {
        this.number = number;
        this.model = number;
    }

    Phone() {
    }

    void sendMessage(String... numbers) {
        System.out.println(Arrays.toString(numbers));
    }

    @Override
    public String toString() {
        return String.format("Номер %s, модель %s, вес %s", number, model, weight);
    }
}

