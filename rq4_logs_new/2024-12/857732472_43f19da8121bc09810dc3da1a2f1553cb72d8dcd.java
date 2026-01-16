package Exceptions_55_56;


import java.util.HashMap;
import java.util.Objects;

public class Car {
    private final String model;
    private final Integer year;

    public Car(String model, int year) {
        this.model = model;
        this.year = year;
    }

    public String getModel() {
        return model;
    }

    public Integer getYear() {
        return year;
    }

    @Override
    public String toString() {
        return "Car{" + "model='" + model + '\'' + ", year=" + year + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (getClass() != o.getClass()) return false;
        Car car = (Car) o;
        return model.equals(car.model) && year.equals(car.year);
    }
    @Override
    public int hashCode() {
        return Objects.hash(model, year);
    }
}
