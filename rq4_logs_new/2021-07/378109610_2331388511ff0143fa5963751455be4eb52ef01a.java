package com.artpro.service;

import com.artpro.model.CivilTransport;

//Такой же метод делаем для Гражданских  самолётов, только проверяем вместимость пассажиров.
public class CivilTransportService {
    public void calculationCivilTransport(CivilTransport civilTransport, int number) {
        if (number <= civilTransport.getNumberOfPassengers()) { //если меньше или равно колличеству пассажиров (посадочных мест)
            System.out.println("Самолёт загружен");
        } else {
            System.out.println("Вам нужен самолёт побольше");
        }
    }
}