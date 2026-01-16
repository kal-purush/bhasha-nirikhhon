package ru.task.deliveryapp.core.domain.sharedkernel;

import ru.task.deliveryapp.exception.ValidationException;

/**
 * Координаты
 */
public class Location {
    public static int MIN_VALUE = 1;
    public static int MAX_VALUE = 10;

    private int x, y;
    private Location() {

    }

    private Location (int x, int y) {
        this.x = x;
        this.y = y;
    }

    public static Location create(int x, int y) {
        if (x < MIN_VALUE || x > MAX_VALUE || y < MIN_VALUE || y > MAX_VALUE) {
            throw new ValidationException("Координаты должна принимать значения в пределах от 1 до 10 включительно");
        }
        Location location = new Location(x, y);
        return location;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    /**
     * расстояние между координатами
     * @param target целевая координата
     * @return расстояние
     */
    public int distanceTo(Location target) {
        return Math.abs(target.getX() - x + target.getY() - y);
    }
}