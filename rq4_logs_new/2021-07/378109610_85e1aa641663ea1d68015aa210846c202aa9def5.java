package com.artpro.task1;

import com.artpro.task1.exceptions.CarExc;
import com.artpro.task1.model.Car;

public class Task1 {
    public static void main(String[] args) {
        Car[] cars = new Car[4];
        cars[0] = new Car("Model1", 100, 1000000);
        cars[1] = new Car("Model2", 200, 2000000);
        cars[2] = new Car("Model3", 300, 3000000);
        cars[3] = new Car("Model4", 400, 4000000);

        for (Car car : cars) {
            try {
                car.start();
            } catch (CarExc exceptionCar) {
                System.out.println(exceptionCar.getMessage());
            } finally {
                System.out.println("Проверка связанная с чётностью сгенерированного числа по условию задачи завершена\n");
            }
        }
    }
}