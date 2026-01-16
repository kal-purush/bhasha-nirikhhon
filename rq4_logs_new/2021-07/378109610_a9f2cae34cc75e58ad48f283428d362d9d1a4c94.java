package com.artpro.service;

import com.artpro.model.Motor;

public class MotorInterface implements MotorService { //прописали связь
    //создаём конструктор
    private Motor motor;

    public MotorInterface(Motor motor) {
        this.motor = motor;
    }

    @Override
    public void start() {
        if (!work()) {
            motor.setWork(true);
            System.out.println("Двигатель не работает");
        } else {
            System.out.println("Двигатель работает");
        }
    }

    @Override
    public void stop() {
        if (!work()) {
            motor.setWork(true);
            System.out.println("Двигатель работает");
        } else {
            System.out.println("Двигатель не работает");
        }
    }

    @Override
    public boolean work() {
        return motor.isWork();
    }


}