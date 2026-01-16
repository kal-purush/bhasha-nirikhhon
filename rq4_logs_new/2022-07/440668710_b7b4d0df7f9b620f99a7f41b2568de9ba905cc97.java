package challenge;

import java.awt.*;
import java.util.Objects;
import java.util.function.Supplier;

public class Car {
    private final String name;
    private final Color color;
    private String license;
    private String location;
    public Car(String name, Color color) {
        this.name = Objects.requireNonNull(name, "Car name cannot be null");
        this.color = Objects.requireNonNull(color, "Car color cannot be null");

    }
    public void assignDriver(String license, String location) {
//        if (license == null || location == null)
//            throw new IllegalArgumentException();
//        this.license = license;
        this.license = MyObjects.requireNonNullElseThrowIAE(license, () -> "Null value not accepted");
        this.location = MyObjects.requireNonNullElseThrow(location, new IllegalArgumentException());

    }
}