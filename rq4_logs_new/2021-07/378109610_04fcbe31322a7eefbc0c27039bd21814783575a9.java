package com.artpro.service;

import com.artpro.model.Car;

//1.1. имплементировали метод distanceCarPassedForAllTime из CarService
public class CarServiceImplements implements CarService {
    private Car car; //не понимаю что сделали

    public CarServiceImplements(Car car) {
        this.car = car;
    }


    @Override
    public void distanceCarPassedForAllTime() {
        //логично вызывать параметр из класса автомобиль (Car) - создадим там поле (1.2)
        //1.3 пишем логику метода
        System.out.println("За всё время машина прошла: " + car.getDistanceCarPassedForAllTime() + " км."); //без private Car car; до метода не достучались бы
    }
}