package com.xml.controller;

import com.xml.dto.CarDto;
import com.xml.service.CarService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(value = "https://localhost:4200")
@RequestMapping(value = "/api/cars", produces = MediaType.APPLICATION_JSON_VALUE)
public class CarController {
    @Autowired
    private CarService carService;

    Logger logger = LoggerFactory.getLogger(CarController.class);

    @PutMapping(value = "")
    public ResponseEntity<?> rate(@RequestBody CarDto carDto) {
        try {
            this.carService.rate(carDto);
        } catch(Exception e) {
            return  new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(HttpStatus.ACCEPTED);
    }

}