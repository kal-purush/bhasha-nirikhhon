import cars.Car;
import cars.PerformanceCar;
import cars.ShowCar;
import races.CasualRace;
import races.DragRace;
import races.DriftRace;

import java.util.ArrayList;
import java.util.Arrays;

public class App {
    public static void main(String[] args) {
        var cars = new ArrayList<Car>();
        var addOns = new ArrayList<String>();
        addOns.add("Аддон 1");
        addOns.add("Аддон 2");
        // todo: как создать ArrayList со значениями?

        System.out.println("===> Тачки:");
        var performanceCar = new PerformanceCar("Ferrari", "F12 Berlinetta", 2022, 490, 2, 10, 20, addOns);
        cars.add(performanceCar);
        System.out.println(performanceCar);
        var showCar = new ShowCar("Aston Martin", "Vanquish", 2023, 390, 3, 10, 20, 5);
        cars.add(showCar);
        System.out.println(showCar);

        System.out.println("\n===> Гонки:");
        var casualRace = new CasualRace(800, "Париж - Дакар", 1_000_000, cars);
        System.out.println(casualRace);
        var dragRace = new DragRace(900, "Уфа - Казань", 900_000, cars);
        System.out.println(dragRace);
        var driftRace = new DriftRace(1000, "Москва - Санкт-Петербург", 1_200_000, cars);
        System.out.println(driftRace);

        System.out.println("\n===> Гараж:");
        var garage = new Garage(Arrays.asList(showCar, performanceCar));
        System.out.println(garage);
        System.out.println("Аддоны до изменения: " + performanceCar.getAddOns());
        garage.modifyCar();
        System.out.println("Аддоны после изменения: " + performanceCar.getAddOns());
    }
}