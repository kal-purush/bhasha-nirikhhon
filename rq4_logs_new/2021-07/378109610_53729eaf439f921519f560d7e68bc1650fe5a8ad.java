package com.artpro.model;
//Создали класс бензобак

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class GasTank {
    //добавляем поля присущие бензобаку
    //по задаче это: общий объем бензобака, сколько бензина сейчас
    private int volumeGasTank; //объём бензобака
    private int levelGasTank; //уровень бензина
//сгенерируем конструктор на основе пока что поля volumeGasTank

    public GasTank(int volumeGasTank) {
        this.volumeGasTank = volumeGasTank;
    }
}