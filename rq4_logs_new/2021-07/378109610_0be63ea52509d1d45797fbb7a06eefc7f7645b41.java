package com.artpro.model;

import lombok.Getter;
import lombok.Setter;

//в пакете моделей создали класс воздушного транспорта, наследующегося от Transport
@Setter
@Getter
public class AirTransport extends Transport {
    //В классе Воздушный добавляются поля:
//- Размах крыльев (м)
//- Минимальная длина взлётно-посадочной полосы для взлёта
    private Integer wingspan; //размах крыльев
    private Integer minRunwayLength; //длина взлётно-посадочной

    //сгенерировали конструктор
    public AirTransport(Integer power, Integer maxSpeed, Integer weight, String brand, Integer wingspan, Integer minRunwayLength) {
        super(power, maxSpeed, weight, brand);
        this.wingspan = wingspan;
        this.minRunwayLength = minRunwayLength;
    }

}
