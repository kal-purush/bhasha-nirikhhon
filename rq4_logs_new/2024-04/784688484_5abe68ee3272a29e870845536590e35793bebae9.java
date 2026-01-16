package ru.task.deliveryapp.core.domain.courieraggregate;

import ru.task.deliveryapp.core.domain.sharedkernel.Location;
import ru.task.deliveryapp.exception.WrongStateException;

import java.util.UUID;

public class Courier {
    private UUID id;
    private String name;
    private Transport transport;
    private Location location;
    private CourierStatus status;

    private Courier() {}

    private Courier(String name, Transport transport) {
        this.id = UUID.randomUUID();
        this.name = name;
        this.transport = transport;
        this.location = Location.create(1, 1);
        this.status = CourierStatus.NOT_AVAILABLE;
    }

    /**
     * Создание курьера.
     *
     * @param name      имя курьера
     * @param transport транспорт курьера
     * @return          курьер
     */
    public static Courier create(String name, Transport transport) {
        return new Courier(name, transport);
    }

    /**
     * Продвижение курьера на один шаг в направлении заказчика.
     *
     * @param targetLocation координаты заказчика.
     */
    public void move(Location targetLocation) {
        if (transport.getSpeed() >= location.distanceTo(targetLocation)) {
            location = targetLocation;
            status = CourierStatus.READY;
        }
        else if (transport.getSpeed() <= (targetLocation.getX() - location.getX())) {
            location = Location.create(location.getX() + transport.getSpeed(), location.getY());
        }
        else {
            location = Location.create(targetLocation.getX(), location.getY() + transport.getSpeed() - (targetLocation.getX() - location.getX()));
        }
    }

    /**
     * Начало работы курьера.
     */
    public void startWork() {
        if (status == CourierStatus.BUSY) {
            throw new WrongStateException("Курьер в статусе 'занят' не может начинать работу.");
        }
        else {
            status = CourierStatus.READY;
        }
    }

    /**
     * Окончание работы курьера.
     */
    public void stopWork() {
        if (status == CourierStatus.BUSY) {
            throw new WrongStateException("Курьер в статусе 'занят' не может заканчивать работу.");
        }
        else {
            status = CourierStatus.NOT_AVAILABLE;
        }
    }

    /**
     * Назначение заказа на курьера.
     */
    public void inWork() {
        if (status == CourierStatus.BUSY) {
            throw new WrongStateException("Курьер уже занят.");
        }
        if (status == CourierStatus.NOT_AVAILABLE) {
            throw new WrongStateException("Курьер недоступен.");
        }
        status = CourierStatus.BUSY;
    }

    /**
     * Подсчёт расстояния до заказчика.
     *
     * @param targetLocation    координаты заказчика
     * @return                  расстояние до заказчика
     */
    public int calculateTimeToPoint(Location targetLocation) {
        int steps = location.distanceTo(targetLocation);
        int timeToPoint = steps / transport.getSpeed() + ((steps % transport.getSpeed()) == 0 ? 0 : 1);
        return timeToPoint;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Transport getTransport() {
        return transport;
    }

    public Location getLocation() {
        return location;
    }

    public CourierStatus getStatus() {
        return status;
    }
}