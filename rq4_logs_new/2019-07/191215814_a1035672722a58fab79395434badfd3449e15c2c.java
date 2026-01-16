package com.epam.brest.summer.courses2019.dao;


import com.epam.brest.summer.courses2019.model.Car;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:test-db.xml", "classpath*:test-dao.xml"})
public class CarDaoJdbcImplTest {

    private static final String CAR_NUMBER = "11-44 AA-1";

    @Autowired
    CarDao carDao;

    @Test
    public void findAll() {
        List<Car> cars = carDao.findAll();
        assertNotNull(cars);
        assertTrue(cars.size() > 0);
    }

    @Test
    public void addCar() {
        Car testCar = new Car();
        testCar.setCarModel("Mersedes");
        testCar.setCarNumber("55-44 AB-7");
        testCar.setLoadCapacity(10);
        testCar.setCarCharacteristics("ref");
        testCar.setCarDriver("Скворцов С.С.");
        Car newCar = carDao.add(testCar);
        assertNotNull(newCar.getCarId());
    }

    @Test
    public void getCarById() {
        Car car = carDao.findById(1).get();
        assertNotNull(car);
        assertTrue(car.getCarId().equals(1));
        assertTrue(car.getCarNumber().equals(CAR_NUMBER));
    }


    @Test
    public void updateCar() {
        Car newCar = new Car();
        newCar = carDao.add(newCar);
        newCar.setCarModel("Volksvagen");
        carDao.update(newCar);
        Car updatedCar = carDao.findById(newCar.getCarId()).get();
        assertTrue(newCar.getCarId().equals(updatedCar.getCarId()));
        assertTrue(newCar.getCarModel().equals(updatedCar.getCarModel()));
    }

    @Test
    public void deleteCar() {
        Car car = new Car();
        car.setCarModel("Volksvagen");
        car = carDao.add(car);
        List<Car> cars = carDao.findAll();
        int sizeBefore = cars.size();
        carDao.delete(car.getCarId());
        assertTrue((sizeBefore - 1) == carDao.findAll().size());
    }

}