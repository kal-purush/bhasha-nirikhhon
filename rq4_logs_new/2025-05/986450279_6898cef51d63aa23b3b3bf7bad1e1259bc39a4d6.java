package com.tomaszek.carscatalog.controllers;

import com.tomaszek.carscatalog.exceptions.CarNotFoundException;
import com.tomaszek.carscatalog.models.Car;
import com.tomaszek.carscatalog.services.CarsCatalogService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/carscatalog")
public class AppController {
    private final CarsCatalogService carsCatalogService;

    public AppController(CarsCatalogService carsCatalogService) {
        this.carsCatalogService = carsCatalogService;
    }

    @GetMapping
    public ResponseEntity<List<Car>> getCars() {
        List<Car> cars = carsCatalogService.getAllCars();
        return ResponseEntity.ok(cars);
    }

    @PostMapping
    public ResponseEntity<String> addCar(@Valid @RequestBody Car car) {
        carsCatalogService.addCar(car);
        return ResponseEntity.status(201).body("Car added");
    }

    @GetMapping("/searchByBrand")
    public ResponseEntity<List<Car>> findCarsByBrand(@RequestParam String brand) {
        List<Car> cars = carsCatalogService.findCarsByBrand(brand);
        if(cars.isEmpty()) {
            throw new CarNotFoundException("No cars found for brand: " + brand);
        }
        return ResponseEntity.ok(cars);
    }

    @GetMapping("/searchByYear")
    public ResponseEntity<List<Car>> findCarsByYear(@RequestParam int fromYear, @RequestParam int toYear) {
        List<Car> cars = carsCatalogService.findCarsByYearRange(fromYear, toYear);
        if(cars.isEmpty()) {
            throw new CarNotFoundException("No cars found between years " + fromYear + " and " + toYear);
        }
        return ResponseEntity.ok(cars);
    }

    @DeleteMapping("/deleteById")
    public ResponseEntity<String> deleteById(@RequestParam int id) {
        Car car =carsCatalogService.findCarById(id).orElseThrow(() -> new CarNotFoundException("No car found with id: " + id));
        carsCatalogService.deleteById(id);
        return ResponseEntity.status(200).body("Car with id " + id + " deleted");
    }
}