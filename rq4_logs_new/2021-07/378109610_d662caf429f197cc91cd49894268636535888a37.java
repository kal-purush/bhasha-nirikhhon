package com.artpro.service;

import com.artpro.model.GasTank;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class GasTankServiceImplements implements GasTankService {
    private final GasTank gasTank; //наш сервис MotorService работает с баком, соответственно создаём объект бак

    //сразу после создания объекта добавляем @AllArgsConstructor ломбока
    @Override
    public boolean isEmpty() {
        return gasTank.getLevelGasTank() == 0;
    }

    @Override
    public int refuel() {
        int refuelVolume = gasTank.getVolumeGasTank() - gasTank.getLevelGasTank();
        gasTank.setLevelGasTank(gasTank.getVolumeGasTank());
        return refuelVolume;

    }
}