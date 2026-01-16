package com.quokkatech.foodtruckmanagement.api.controllers;

import com.quokkatech.foodtruckmanagement.api.request.FoodTruckRequest;
import com.quokkatech.foodtruckmanagement.api.response.FoodTruckResponse;
import com.quokkatech.foodtruckmanagement.api.response.UserSessionResponse;
import com.quokkatech.foodtruckmanagement.application.exceptions.TruckAlreadyExistsException;
import com.quokkatech.foodtruckmanagement.application.services.FoodTruckService;
import com.quokkatech.foodtruckmanagement.domain.entities.FoodTruck;
import com.quokkatech.foodtruckmanagement.domain.entities.User;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/truckRegistrations")
public class FoodTruckController {

    private final FoodTruckService foodTruckService;

    private static final Logger logger = LoggerFactory.getLogger(UserSessionController.class);
    @Autowired
    public FoodTruckController(FoodTruckService foodTruckService){
        this.foodTruckService=foodTruckService;
    }

    @PostMapping("")
    public ResponseEntity<FoodTruckResponse> registerFoodTruck(@RequestBody FoodTruckRequest foodTruckRequest){
        try {
            ModelMapper modelMapper = new ModelMapper();
            FoodTruck foodTruck = modelMapper.map(foodTruckRequest, FoodTruck.class);
            foodTruckService.createFoodTruck(foodTruck);

            FoodTruckResponse foodTruckResponse = new FoodTruckResponse(foodTruckRequest.getTruckId(), foodTruckRequest.getTruckName(), foodTruckRequest.getUser());

            return ResponseEntity.status(HttpStatus.CREATED).body(foodTruckResponse);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new FoodTruckResponse(foodTruckRequest.getTruckId(), foodTruckRequest.getTruckName(), foodTruckRequest.getUser()));
        }

    }

    @GetMapping("/{truckId}")
    public ResponseEntity<FoodTruck> getFoodTruckById(@PathVariable Long truckId){
        logger.info("UserController.findById - Finding truck with ID: {}", truckId);

        Optional<FoodTruck> foodTruckOptional = foodTruckService.findById(truckId);

        return foodTruckOptional
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
    @GetMapping("/truck/{truckName}")
    public ResponseEntity<FoodTruck> getFoodTruckByName(@PathVariable String name){
        logger.info("UserController.findByName - Finding truck with name: {}", name);

        Optional<FoodTruck> foodTruckOptional = foodTruckService.findByName(name);

        return foodTruckOptional
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}