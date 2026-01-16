package com.bin448.backend.converter;

import com.bin448.backend.entity.CarType;
import com.bin448.backend.entity.DTOentity.CarTypeDTO;
import com.bin448.backend.repository.CarRepository;
import com.bin448.backend.repository.CarTypeRepository;
import org.springframework.stereotype.Component;

@Component
public class CarTypeConverter extends AbstractConverter {

    private static CarTypeRepository ct;
    private static CarRepository ccr;

    public CarTypeConverter(CarRepository crr,CarTypeRepository ct){
        this.ct = ct;
        this.ccr = crr;
    }

    public static CarType toEntity(CarTypeDTO c){
        CarType cc = new CarType();
        cc.setCar(ccr.getCarByCarId(c.getCarId()));
        cc.setSeats(c.getSeats());
        cc.setId(c.getId());
        cc.setName(c.getName());
        return cc;
    }

    public static CarTypeDTO fromEntity(CarType c){
        CarTypeDTO ctt = new CarTypeDTO();
        ctt.setCarId(c.getCar().getCarId());
        ctt.setId(c.getId());
        ctt.setName(c.getName());
        ctt.setSeats(c.getSeats());
        return ctt;
    }

}