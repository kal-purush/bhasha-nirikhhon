package com.epam.brest.summer.courses2019.service;

import com.epam.brest.summer.courses2019.dao.CarDao;
import com.epam.brest.summer.courses2019.model.Car;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CarServiceImpl implements CarService{

    private static final Logger LOGGER = LoggerFactory.getLogger(CarServiceImpl.class);

    private CarDao carDao;

    public CarServiceImpl(CarDao carDao){
        this.carDao = carDao;
    }

    @Override
    public Car add(Car car) {
        LOGGER.debug("add({})", car);
        return carDao.add(car);
    }

    @Override
    public void update(Car car) {
        LOGGER.debug("update({})", car);
        carDao.update(car);
    }

    @Override
    public void delete(Integer carId) {
        LOGGER.debug("delete({})", carId);
        carDao.delete(carId);
    }

    @Override
    public List<Car> findAll() {
        LOGGER.debug("Find all cars");
        return carDao.findAll();
    }

    @Override
    public Car findById(Integer carId) {
        LOGGER.debug("findById({})", carId);
        return carDao.findById(carId)
                .orElseThrow(() -> new RuntimeException("Failed to get car from DB"));
    }
}