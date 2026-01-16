package com.artpro.service;

import com.artpro.model.CargoTransport;

//Для грузового разработать метод который проверит можно ли загрузить в
//него xxx груза
//Метод должен проверять если это кол-во груза помещается в грузовик то
//выводит в консоль ”Грузовик загружен”, если кол-во груза которое нужно
//загрузить больше чем то которое может влезть в наш грузовик то выводим
//“Вам нужен грузовик побольше ”.
public class CargoTransportService {
    public void calculationCargoTransportService(CargoTransport cargoTransport, double cargo) {

        if (cargo == cargoTransport.getLoadCapacity() || cargo < cargoTransport.getLoadCapacity()) {
            System.out.println("Грузовик загружен");
        } else {
            System.out.println("Вам нужен грузовик побольше");
        }
    }
}