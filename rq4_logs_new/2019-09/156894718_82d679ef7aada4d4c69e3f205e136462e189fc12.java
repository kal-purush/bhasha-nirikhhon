package com.bin448.backend.converter;

import com.bin448.backend.entity.CarDiscount;
import com.bin448.backend.entity.DTOentity.CarDiscountDTO;
import com.bin448.backend.repository.CarDiscountRepository;
import com.bin448.backend.repository.CarRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CarDiscountConverter {
    @Autowired
    private static CarDiscountRepository carDiscountRepository;
    @Autowired
    private static CarRepository carRepository;

    public static CarDiscount toEntity(CarDiscountDTO carDiscountDTO){
        CarDiscount carDiscount = new CarDiscount();
        carDiscount.setCar(carRepository.getCarByCarId(carDiscountDTO.getId()));
        carDiscount.setEndDate(carDiscountDTO.getEndDate());
        carDiscount.setStartDate(carDiscountDTO.getStartDate());
        carDiscount.setId(carDiscountDTO.getId());
        carDiscount.setPrice(carDiscountDTO.getPrice());

        return carDiscount;
    }

    public static  CarDiscountDTO fromEntity(CarDiscount carDiscount){
        CarDiscountDTO carDiscountDTO = new CarDiscountDTO();
        carDiscountDTO.setCarId(carDiscount.getCar().getCarId());
        carDiscountDTO.setEndDate(carDiscount.getEndDate());
        carDiscountDTO.setStartDate(carDiscount.getStartDate());
        carDiscountDTO.setId(carDiscount.getId());
        carDiscountDTO.setPercent(carDiscount.getRateOfDiscount());
        carDiscountDTO.setPrice(carDiscount.getPrice());
        carDiscountDTO.setModel(carDiscount.getCar().getModel());
        carDiscountDTO.setNos(carDiscount.getCar().getNumOfSeats());
        return  carDiscountDTO;
    }
}