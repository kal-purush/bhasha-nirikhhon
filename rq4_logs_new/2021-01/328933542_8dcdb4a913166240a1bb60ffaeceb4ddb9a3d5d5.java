package com.mate.dao;

import com.mate.db.Storage;
import com.mate.lib.Dao;
import com.mate.model.Car;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Dao
public class CarDaoImpl implements CarDao {
    @Override
    public Car create(Car car) {
        Storage.addCar(car);
        return car;
    }

    @Override
    public Optional<Car> get(Long id) {
        return Storage.cars.stream()
                .filter(x -> Objects.equals(x.getId(),id))
                .findFirst();
    }

    @Override
    public List<Car> getAll() {
        return Storage.cars;
    }

    @Override
    public Car update(Car car) {
        Car old = get(car.getId()).get();
        Storage.cars.set(Storage.cars.indexOf(old), car);
        return old;
    }

    @Override
    public boolean delete(Long id) {
        return Storage.cars.removeIf(x -> x.getId().equals(id));
    }
}